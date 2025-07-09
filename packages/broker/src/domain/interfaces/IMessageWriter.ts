import type { MessageMetadata } from "../entities/MessageMetadata";

export interface IMessageWriter {
  write(message: Buffer, meta: MessageMetadata): Promise<number | undefined>;
}
