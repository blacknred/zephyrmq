export interface IQueueDeleter {
  delete(id: number): Promise<void>;
}