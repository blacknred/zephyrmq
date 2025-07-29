import type { MessageMetadata } from "@domain/entities/MessageMetadata";
import type { IDLQEntry } from "@domain/interfaces/dlq/IDLQEntry";

export interface IDLQConsumer<Data> {
  id: number;
  consume(): Promise<IDLQEntry<Data>[]>;
  replayDlq(
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ): Promise<number>;
}
