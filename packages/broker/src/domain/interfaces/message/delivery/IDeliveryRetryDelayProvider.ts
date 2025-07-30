export interface IDeliveryRetryDelayProvider {
  getDelay(messageId: number): Promise<number>;
}