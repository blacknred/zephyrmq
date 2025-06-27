import type { MessageMetadata } from "../models/MessageMetadata";

export interface IMessageWriter {
  write(message: Buffer, meta: MessageMetadata): Promise<number | undefined>;
}
