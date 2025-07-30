import type { ISubscriptionListener } from "./ISubscriptionListener";

export interface ISubscriptionGetter {
  get<Data>(
    consumerId: number
  ): Promise<ISubscriptionListener<Data> | undefined>;
}
