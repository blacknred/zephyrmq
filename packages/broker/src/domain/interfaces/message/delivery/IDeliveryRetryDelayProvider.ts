export interface IDeliveryRetryDelayProvider {
  getDelay(messageId: number): number;
}