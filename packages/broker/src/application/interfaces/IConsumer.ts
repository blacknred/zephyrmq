export type ISubscriptionListener<Data> = (message: Data) => Promise<void>;

export interface IConsumerConfig {
  routingKeys?: string[];
  groupId?: string;
  limit?: number;
  noAck?: boolean;
}

export interface IConsumer<Data> {
  id: number;
  consume(): Promise<Data[]>;
  ack(messageId?: number): Promise<number[]>;
  nack(messageId?: number, requeue?: boolean): Promise<number>;
  subscribe(listener: ISubscriptionListener<Data>): void;
  unsubscribe(): void;
}
