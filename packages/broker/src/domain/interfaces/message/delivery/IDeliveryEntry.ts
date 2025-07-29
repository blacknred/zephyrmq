export interface IDeliveryEntry {
  awaited: number;
  attempts: number;
}

export interface IAwaitedDeliveryCreator {}
export interface IDeliveryRetryBackoffGetter {}