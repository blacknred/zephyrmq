export interface IDeliveryCreator {
  create(messageId: number, expectedDeliveries: number): void;
}