import type { TransferListItem } from "node:worker_threads";

export type WorkerRequest = {
  id: number;
  method: string;
  args: any[];
};

export type WorkerResponse<T = any> = {
  id: number;
  result?: T;
  error?: string;
};

export interface IWorkerPool {
  send<T>(
    method: string,
    args: any[],
    transferList?: TransferListItem[]
    // worker?: any
  ): Promise<T>;
  sendToAll<T>(
    method: string,
    args: any[],
    transferList?: TransferListItem[]
  ): Promise<Awaited<T>[]>;
}
