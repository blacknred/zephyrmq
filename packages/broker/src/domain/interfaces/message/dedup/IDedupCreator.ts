export interface IDedupCreator {
  create(consumerId: number, messageId: number): Promise<void>;
}