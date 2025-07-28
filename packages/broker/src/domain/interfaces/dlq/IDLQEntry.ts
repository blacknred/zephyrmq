import type { MessageMetadata } from "@domain/entities/MessageMetadata";
import type { DLQReason } from "./dlq/DLQReason";

export interface IDLQEntry<Data> {
  reason: DLQReason;
  message: Data;
  meta: MessageMetadata;
}
