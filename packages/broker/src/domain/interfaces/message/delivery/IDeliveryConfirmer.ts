export interface IDeliveryConfirmer {
  confirm(messageId: number): Promise<void>;
}