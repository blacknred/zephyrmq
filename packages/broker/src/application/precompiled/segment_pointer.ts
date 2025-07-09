import { SegmentPointer } from "../..";
import type { ISchema } from "../types";

export const segmentPointerSchema: ISchema<SegmentPointer> = {
  serialize(pointer: SegmentPointer): Buffer {
    const buffer = Buffer.alloc(16);
    buffer.writeInt32BE(pointer.segmentId, 0);
    buffer.writeInt32BE(pointer.offset, 4);
    buffer.writeInt32BE(pointer.length, 8);
    buffer.writeInt32BE(pointer.messageOffset, 12);
    return buffer;
  },

  deserialize(buffer: Buffer): SegmentPointer {
    const pointer = new SegmentPointer();
    pointer.segmentId = buffer.readInt32BE(0);
    pointer.offset = buffer.readInt32BE(4);
    pointer.length = buffer.readInt32BE(8);
    pointer.messageOffset = buffer.readInt32BE(12);
    return pointer;
  },
};
