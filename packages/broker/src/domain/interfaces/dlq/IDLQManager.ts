import type { MessageMetadata } from "@domain/entities/MessageMetadata";
import type { DLQReason } from "./DLQReason";
import type { IDLQEntry } from "./IDLQEntry";

export interface IDLQManager<T> {
  enqueue(meta: MessageMetadata, reason: DLQReason): void;
  createReader(): AsyncGenerator<IDLQEntry<T>, void, unknown>;
  replayMessages(
    handler: (message: T, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ): Promise<number>;
  getMetrics(): {
    size: number;
  };
}
