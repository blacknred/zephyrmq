import Buffer from "node:buffer";
import { MessageMetadata } from "../25.ts";
import { ICodec } from "./binary_codec";
import { Worker } from "node:worker_threads";
import os from "os";
import { join } from "path";

type WorkerRequest = {
  id: number;
  method: string;
  args: any[];
};

type WorkerResponse<T = any> = {
  id: number;
  result?: T;
  error?: string;
};

export class ThreadedBinaryCodec implements ICodec {
  private workerPath = join(__dirname, "worker", "binary_codec.worker.ts");
  private pending = new Map<
    number,
    { resolve: (value: any) => void; reject: (reason?: any) => void }
  >();
  private workers: Worker[] = [];
  private nextId = 0;

  constructor(workersCount = os.cpus().length) {
    for (let i = 0; i < workersCount; i++) {
      const worker = new Worker(this.workerPath);
      worker.on("message", (response: WorkerResponse) => {
        const handlers = this.pending.get(response.id);
        if (!handlers) return;

        if (response.error) {
          handlers.reject(new Error(response.error));
        } else {
          // Wrap ArrayBuffer result in Buffer
          handlers.resolve(
            response.result instanceof ArrayBuffer
              ? Buffer.from(response.result)
              : response.result
          );
        }

        this.pending.delete(response.id);
      });

      this.workers.push(worker);
    }
  }

  destroy(): void {
    for (const worker of this.workers) {
      worker.terminate();
    }
    this.workers = [];
  }

  private getNextWorker(): Worker {
    // TODO: use idle instead of
    const idx = Math.floor(Math.random() * this.workers.length);
    return this.workers[idx];
  }

  private send<T>(
    method: string,
    args: any[],
    transferables?: Transferable[]
  ): Promise<T> {
    const id = this.nextId++;
    const worker = this.getNextWorker();

    worker.postMessage<WorkerRequest>({ id, method, args }, transferables);

    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
  }

  async encode<T>(data: T): Promise<Buffer> {
    // Worker returns ArrayBuffer → wrap in Buffer before resolving
    return this.send<Buffer>("encode", [data]);
  }

  async decode<T>(buffer: Buffer): Promise<T> {
    const arrayBuffer = buffer.buffer;
    const byteOffset = buffer.byteOffset;
    const byteLength = buffer.byteLength;

    return this.send<T>("decode", [{ byteOffset, byteLength }], [arrayBuffer]);
  }

  async encodeMetadata(meta: MessageMetadata): Promise<Buffer> {
    return this.send<Buffer>("encodeMetadata", [meta]);
  }

  async decodeMetadata<K extends keyof MessageMetadata>(
    buffer: Buffer,
    keys?: K[]
  ): Promise<Pick<MessageMetadata, K>> {
    const arrayBuffer = buffer.buffer;
    const byteOffset = buffer.byteOffset;
    const byteLength = buffer.byteLength;

    return this.send<Pick<MessageMetadata, K>>(
      "decodeMetadata",
      [{ byteOffset, byteLength }, keys],
      [arrayBuffer]
    );
  }

  async updateMetadata(
    buffer: Buffer,
    partialMeta: Partial<MessageMetadata>
  ): Promise<Buffer> {
    const arrayBuffer = buffer.buffer;
    const byteOffset = buffer.byteOffset;
    const byteLength = buffer.byteLength;

    return this.send<Buffer>(
      "updateMetadata",
      [{ byteOffset, byteLength }, partialMeta],
      [arrayBuffer]
    );
  }
}
