export interface ISubscriptionDeleter {
  delete(consumerId: number): Promise<void>;
}