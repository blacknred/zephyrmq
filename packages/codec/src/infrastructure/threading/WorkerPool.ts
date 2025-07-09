import type {
  IWorkerPool,
  WorkerRequest,
  WorkerResponse,
} from "@domain/interfaces/IWorkerPool";
import type { TransferListItem } from "node:worker_threads";
import { Worker } from "node:worker_threads";
import os from "os";

interface IPendingTask {
  resolve: (value: any) => void;
  reject: (reason?: any) => void;
}

export class WorkerPool implements IWorkerPool {
  private pending = new Map<number, IPendingTask>();
  private workers: Worker[] = [];
  private pendingRequests: number[] = [];
  private nextId = 0;

  constructor(
    workerPath: string,
    workerData?: unknown,
    workersCount = os.cpus().length
  ) {
    for (let i = 0; i < workersCount; i++) {
      const worker = new Worker(workerPath, {
        workerData,
      });

      worker.on("message", (response: WorkerResponse) => {
        const handlers = this.pending.get(response.id);
        if (!handlers) return;

        const workerIndex = this.workers.indexOf(worker);
        if (workerIndex !== -1) {
          this.pendingRequests[workerIndex]--;
        }

        if (response.error) {
          handlers.reject(new Error(response.error));
        } else {
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

    this.pendingRequests = new Array(this.workers.length).fill(0);
  }

  private getNextWorker(): Worker {
    let idx = 0;
    for (let i = 1; i < this.pendingRequests.length; i++) {
      if (this.pendingRequests[i] < this.pendingRequests[idx]) {
        idx = i;
      }
    }
    return this.workers[idx];
  }

  send<T>(
    method: string,
    args: any[],
    transferList?: TransferListItem[],
    worker = this.getNextWorker()
  ): Promise<T> {
    const id = this.nextId++;
    worker.postMessage({ id, method, args } as WorkerRequest, transferList);

    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
  }

  sendToAll<T>(
    method: string,
    args: any[],
    transferList?: TransferListItem[]
  ): Promise<Awaited<T>[]> {
    return Promise.all(
      this.workers.map((worker) => {
        return this.send<T>(method, args, transferList, worker);
      })
    );
  }
}
