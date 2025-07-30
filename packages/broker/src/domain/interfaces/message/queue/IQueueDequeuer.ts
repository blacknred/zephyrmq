export interface IQueueDequeuer {
  dequeue(id: number): Promise<number | undefined>;
}