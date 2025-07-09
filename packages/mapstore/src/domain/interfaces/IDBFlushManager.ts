export type IFlushTask = () => Promise<void>;

export interface IDBFlushManager {
  register(task: IFlushTask): void;
  unregister(task: IFlushTask): void;
  commit(): void;
  close(): void;
}

export interface IDBFlushManagerConfig {
  persistThresholdMs?: number;
  maxPendingFlushes?: number;
  memoryUsageThresholdMB?: number;
}
