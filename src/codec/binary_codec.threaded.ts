import { Worker } from "node:worker_threads";
import { join } from "node:path";
import Buffer from "node:buffer";
import os from "node:os";
import { MessageMetadata } from "../22";
import { ICodec } from "./binary_codec";

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

// src/codecs/threaded-binary-codec.ts
// Even though the threaded version has slightly higher latency per message,
// it **unblocks the main thread**, allowing you to scale better under real-world concurrency.
export class ThreadedBinaryCodec implements ICodec {
  private workerPath = join(__dirname, "worker", "binary_codec.worker.ts");
  private pending = new Map<number, (result: any) => void>();
  private workers: Worker[] = [];
  private nextId = 0;

  constructor() {
    for (let i = 0; i < os.cpus(); i++) {
      const worker = new Worker(this.workerPath);
      worker.on("message", (response: WorkerResponse) => {
        const resolve = this.pending.get(response.id)!;
        if (response.error) throw new Error(response.error);
        resolve(response.result);
        this.pending.delete(response.id);
      });

      this.workers.push(worker);
    }
  }

  private getNextWorker(): Worker {
    const idx = Math.floor(Math.random() * this.workers.length);
    return this.workers[idx];
  }

  private send<T>(method: string, args: any[]): Promise<T> {
    const id = this.nextId++;
    const worker = this.getNextWorker();
    worker.postMessage<WorkerRequest>({ id, method, args });
    return new Promise((resolve, reject) => {
      this.pending.set(id, resolve);
    });
  }

  async encode<T>(data: T): Promise<Buffer> {
    return this.send<Buffer>("encode", [data]);
  }

  async decode<T>(buffer: Buffer): Promise<T> {
    return this.send<T>("decode", [buffer]);
  }

  async encodeMetadata(meta: MessageMetadata): Promise<Buffer> {
    return this.send<Buffer>("encodeMetadata", [meta]);
  }

  async decodeMetadata<K extends keyof MessageMetadata>(
    buffer: Buffer,
    keys?: K[]
  ): Promise<Pick<MessageMetadata, K>> {
    return this.send<Pick<MessageMetadata, K>>("decodeMetadata", [
      buffer,
      keys,
    ]);
  }

  async updateMetadata(
    buffer: Buffer,
    partialMeta: Partial<MessageMetadata>
  ): Promise<Buffer> {
    return this.send<Buffer>("updateMetadata", [buffer, partialMeta]);
  }
}
