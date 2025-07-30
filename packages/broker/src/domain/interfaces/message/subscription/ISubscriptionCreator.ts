import type { ISubscriptionListener } from "./ISubscriptionListener";

export interface ISubscriptionCreator {
  create<Data>(consumerId: number,
    listener: ISubscriptionListener<Data>,
    noAck?: boolean): Promise<void>;
}