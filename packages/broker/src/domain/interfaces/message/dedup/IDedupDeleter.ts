export interface IDedupDeleter {
  delete(consumerId: number, messageId: number): Promise<void>;
}