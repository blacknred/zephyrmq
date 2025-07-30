export interface IAckCreator {
  create(consumerId: number, messageId: number): Promise<void>;
}