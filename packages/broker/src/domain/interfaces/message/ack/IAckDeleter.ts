export interface IAckDeleter {
  delete(consumerId: number, messageId?: number): Promise<void>;
}