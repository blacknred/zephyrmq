import { MessageMetadata } from "../..";
import type { ISchema } from "../types";

export const messageMetadataSchema: ISchema<MessageMetadata> = {
  serialize(meta: MessageMetadata): Buffer {
    const corrIdBuf = meta.correlationId
      ? Buffer.from(meta.correlationId, "utf8")
      : Buffer.alloc(0);
    const routeKeyBuf = meta.routingKey
      ? Buffer.from(meta.routingKey, "utf8")
      : Buffer.alloc(0);
    const dedupIdBuf = meta.dedupId
      ? Buffer.from(meta.dedupId, "utf8")
      : Buffer.alloc(0);
    const topicBuf = Buffer.from(meta.topic, "utf8");

    const flags = meta.flags;

    let fixedSize = 4 + 8 + 4 + 1; // id(4), ts(8), producerId(4), flags(1)

    if (flags & 0x01) fixedSize += 1; // priority
    if (flags & 0x02) fixedSize += 4; // ttl
    if (flags & 0x04) fixedSize += 4; // ttd

    const variableSize =
      1 +
      topicBuf.length +
      (corrIdBuf.length > 0 ? 1 + corrIdBuf.length : 0) +
      (routeKeyBuf.length > 0 ? 1 + routeKeyBuf.length : 0) +
      (dedupIdBuf.length > 0 ? 1 + dedupIdBuf.length : 0);

    const buffer = Buffer.allocUnsafe(fixedSize + variableSize);
    let offset = 0;

    buffer.writeUInt32BE(meta.id, offset);
    offset += 4;
    buffer.writeDoubleBE(meta.ts, offset);
    offset += 8;
    buffer.writeUInt32BE(meta.producerId, offset);
    offset += 4;
    buffer.writeUInt8(flags, offset++);

    if (flags & 0x01) {
      buffer.writeUInt8(meta.priority!, offset++);
    }
    if (flags & 0x02) {
      buffer.writeUInt32BE(meta.ttl!, offset);
      offset += 4;
    }
    if (flags & 0x04) {
      buffer.writeUInt32BE(meta.ttd!, offset);
      offset += 4;
    }

    // Variable fields
    buffer.writeUInt8(topicBuf.length, offset++);
    topicBuf.copy(buffer, offset);
    offset += topicBuf.length;

    if (flags & 0x10) {
      buffer.writeUInt8(corrIdBuf.length, offset++);
      corrIdBuf.copy(buffer, offset);
      offset += corrIdBuf.length;
    }

    if (flags & 0x20) {
      buffer.writeUInt8(routeKeyBuf.length, offset++);
      routeKeyBuf.copy(buffer, offset);
      offset += routeKeyBuf.length;
    }

    if (flags & 0x40) {
      buffer.writeUInt8(dedupIdBuf.length, offset++);
      dedupIdBuf.copy(buffer, offset);
      offset += dedupIdBuf.length;
    }

    return buffer;
  },

  deserialize(buffer: Buffer): MessageMetadata {
    const meta = new MessageMetadata();
    let offset = 0;

    meta.id = buffer.readUInt32BE(offset);
    offset += 4;
    meta.ts = buffer.readDoubleBE(offset);
    offset += 8;
    meta.producerId = buffer.readUInt32BE(offset);
    offset += 4;
    const flags = buffer.readUInt8(offset++);

    if (flags & 0x01) {
      meta.priority = buffer.readUInt8(offset++);
    } else {
      offset++;
    }

    if (flags & 0x02) {
      meta.ttl = buffer.readUInt32BE(offset);
      offset += 4;
    } else {
      offset += 4;
    }

    if (flags & 0x04) {
      meta.ttd = buffer.readUInt32BE(offset);
      offset += 4;
    } else {
      offset += 4;
    }

    const topicLen = buffer.readUInt8(offset++);
    meta.topic = buffer.toString("utf8", offset, offset + topicLen);
    offset += topicLen;

    if (flags & 0x10) {
      const corrIdLen = buffer.readUInt8(offset++);
      meta.correlationId = buffer.toString("utf8", offset, offset + corrIdLen);
      offset += corrIdLen;
    }

    if (flags & 0x20) {
      const routeKeyLen = buffer.readUInt8(offset++);
      meta.routingKey = buffer.toString("utf8", offset, offset + routeKeyLen);
      offset += routeKeyLen;
    }

    if (flags & 0x40) {
      const dedupIdLen = buffer.readUInt8(offset++);
      meta.dedupId = buffer.toString("utf8", offset, offset + dedupIdLen);
      offset += dedupIdLen;
    }

    return meta;
  },
};
