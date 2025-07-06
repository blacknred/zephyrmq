type IFlushTask = () => Promise<void>;

export interface IFlushManager {
  register(task: IFlushTask): void;
  unregister(task: IFlushTask): void;
  commit(): void;
  close(): void;
}
