export interface IQueueCreator {
  create(id: number): Promise<void>;
}