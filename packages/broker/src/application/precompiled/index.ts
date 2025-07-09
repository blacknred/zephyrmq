import type { ISchema } from "../types";
import { messageMetadataSchema } from "./message_metadata";
import { segmentPointerSchema } from "./segment_pointer";

export enum PrecompiledSchema {
  SegmentPointerSchema = "segment_pointer_schema",
  MessageMetadataSchema = "message_metadata_schema",
}

const schemas: Record<PrecompiledSchema, ISchema<unknown>> = {
  [PrecompiledSchema.MessageMetadataSchema]: messageMetadataSchema,
  [PrecompiledSchema.SegmentPointerSchema]: segmentPointerSchema,
};

export default schemas;
