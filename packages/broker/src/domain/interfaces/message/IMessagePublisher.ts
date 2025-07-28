import type { MessageMetadata } from "@domain/entities/MessageMetadata";

export interface IMessagePublisher {
  publish(meta: MessageMetadata, skipDLQ?: boolean): Promise<void>;
}