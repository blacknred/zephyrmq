import Buffer from "node:buffer";
import { MessageMetadata } from "../22";

// SRC/MESSAGE/CODECS/
export interface ICodec {
  encode<T>(data: T): Promise<Buffer>;
  decode<T>(buffer: Buffer): Promise<T>;
  encodeMetadata(meta: MessageMetadata): Promise<Buffer>;
  decodeMetadata<K extends keyof MessageMetadata>(
    buffer: Buffer,
    keys?: K[]
  ): Promise<Pick<MessageMetadata, K>>;
  updateMetadata(
    buffer: Buffer,
    partialMeta: Partial<MessageMetadata>
  ): Promise<Buffer>;
}

// Binary packing codec.
// vs json: -50% ram, 3x encode, 5x decode
// binary_codec(+ajv precompiled validation) vs protobuf(+validation): 2x faster, -10% size, -40% ram;
// but no cross-lang, no schema-evolution, no faster if mostly optional keys
export class BinaryCodec implements ICodec {
  // Encodes payload only
  encode<T>(data: T): Buffer {
    const payloadJson = JSON.stringify(data);
    return Buffer.from(payloadJson);
  }

  // Decodes payload only
  decode<T>(buffer: Buffer): T {
    const payload = JSON.parse(buffer.toString("utf8")) as T;
    return payload;
  }

  // Encodes metadata into a separate buffer
  encodeMetadata(meta: MessageMetadata): Buffer {
    // Calculate size for all optional fields
    const routingKeyBuf = meta.routingKey
      ? Buffer.from(meta.routingKey, "utf8")
      : Buffer.alloc(0);

    const topicBuf = Buffer.from(meta.topic, "utf8");
    const corrIdBuf = meta.correlationId
      ? Buffer.from(meta.correlationId, "utf8")
      : Buffer.alloc(0);

    // Allocate buffer (fixed + variable-length)
    const buffer = Buffer.allocUnsafe(
      46 + // Fixed-size header
        1 +
        topicBuf.length + // topic (length-prefixed)
        (corrIdBuf.length > 0 ? 1 + corrIdBuf.length : 0) +
        (routingKeyBuf.length > 0 ? 1 + routingKeyBuf.length : 0) +
        4 // size (int32)
    );

    let offset = 0;

    // --- MessageMetadata Header (46 bytes fixed) ---
    buffer.writeUInt32BE(meta.id, offset);
    offset += 4; // id
    buffer.writeDoubleBE(meta.ts, offset);
    offset += 8; // ts
    buffer.writeUInt32BE(meta.producerId, offset);
    offset += 4; // producerId

    // flags
    const flags =
      (meta.priority !== undefined ? 0x01 : 0) |
      (meta.ttl !== undefined ? 0x02 : 0) |
      (meta.ttd !== undefined ? 0x04 : 0) |
      (meta.batchId !== undefined ? 0x08 : 0) |
      (meta.correlationId !== undefined ? 0x10 : 0) |
      (meta.routingKey !== undefined ? 0x20 : 0);

    buffer.writeUInt8(flags, offset);
    offset += 1;

    if (flags & 0x01) buffer.writeUInt8(meta.priority!, offset++); // priority
    if (flags & 0x02) {
      buffer.writeUInt32BE(meta.ttl!, offset);
      offset += 4;
    } // ttl
    if (flags & 0x04) {
      buffer.writeUInt32BE(meta.ttd!, offset);
      offset += 4;
    } // ttd
    if (flags & 0x08) {
      buffer.writeUInt32BE(meta.batchId!, offset);
      offset += 4;
      buffer.writeUInt16BE(meta.batchIdx!, offset);
      offset += 2;
      buffer.writeUInt16BE(meta.batchSize!, offset);
      offset += 2;
    }
    buffer.writeUInt8(meta.attempts, offset++); // attempts
    if (meta.consumedAt !== undefined) {
      buffer.writeDoubleBE(meta.consumedAt, offset);
      offset += 8;
    }

    // --- Variable-Length Fields ---
    // topic
    buffer.writeUInt8(topicBuf.length, offset++);
    topicBuf.copy(buffer, offset);
    offset += topicBuf.length;

    // correlationId
    if (flags & 0x10) {
      buffer.writeUInt8(corrIdBuf.length, offset++);
      corrIdBuf.copy(buffer, offset);
      offset += corrIdBuf.length;
    }

    // routingKey
    if (flags & 0x20) {
      buffer.writeUInt8(routingKeyBuf.length, offset++);
      routingKeyBuf.copy(buffer, offset);
      offset += routingKeyBuf.length;
    }

    // size
    buffer.writeUInt32BE(meta.size, offset);
    offset += 4;

    // needAcks
    buffer.writeUInt32BE(meta.needAcks, offset);
    offset += 4;

    return buffer.slice(0, offset);
  }

  // Decodes full/partial metadata from buffer
  decodeMetadata<K extends keyof MessageMetadata>(
    buffer: Buffer,
    keys?: K[]
  ): Pick<MessageMetadata, K> {
    const meta: Partial<MessageMetadata> = {};
    let offset = 0;

    // Helper to read only what we need
    const shouldRead = (key: keyof MessageMetadata) =>
      // @ts-ignore
      !keys || keys.includes(key);

    // --- Fixed-width fields ---
    if (shouldRead("id")) {
      meta.id = buffer.readUInt32BE(offset);
    }
    offset += 4;

    if (shouldRead("ts")) {
      meta.ts = buffer.readDoubleBE(offset);
    }
    offset += 8;

    if (shouldRead("producerId")) {
      meta.producerId = buffer.readUInt32BE(offset);
    }
    offset += 4;

    const flags = buffer.readUInt8(offset++);
    // @ts-ignore
    meta.flags = flags; // synthetic property
    offset++;

    // Optional fixed-size fields
    if (flags & 0x01 && shouldRead("priority")) {
      meta.priority = buffer.readUInt8(offset);
      offset += 1;
    } else if (!shouldRead("priority")) {
      if (flags & 0x01) offset += 1;
    }

    if (flags & 0x02 && shouldRead("ttl")) {
      meta.ttl = buffer.readUInt32BE(offset);
      offset += 4;
    } else if (!shouldRead("ttl")) {
      if (flags & 0x02) offset += 4;
    }

    if (flags & 0x04 && shouldRead("ttd")) {
      meta.ttd = buffer.readUInt32BE(offset);
      offset += 4;
    } else if (!shouldRead("ttd")) {
      if (flags & 0x04) offset += 4;
    }

    if (flags & 0x08 && shouldRead("batchId")) {
      meta.batchId = buffer.readUInt32BE(offset);
      offset += 4;
      meta.batchIdx = buffer.readUInt16BE(offset);
      offset += 2;
      meta.batchSize = buffer.readUInt16BE(offset);
      offset += 2;
    } else if (!shouldRead("batchId")) {
      if (flags & 0x08) offset += 8;
    }

    if (shouldRead("attempts")) {
      meta.attempts = buffer.readUInt8(offset);
    }
    offset++;

    if (flags & 0x10 && shouldRead("consumedAt")) {
      meta.consumedAt = buffer.readDoubleBE(offset);
      offset += 8;
    } else if (!shouldRead("consumedAt")) {
      if (flags & 0x10) offset += 8;
    }

    // --- Variable-Length Fields ---

    // topic
    const topicLen = buffer.readUInt8(offset++);
    if (shouldRead("topic")) {
      meta.topic = buffer.toString("utf8", offset, offset + topicLen);
    }
    offset += topicLen;

    // correlationId
    if (flags & 0x10) {
      const corrIdLen = buffer.readUInt8(offset++);
      if (shouldRead("correlationId")) {
        meta.correlationId = buffer.toString(
          "utf8",
          offset,
          offset + corrIdLen
        );
      }
      offset += corrIdLen;
    }

    // routingKey
    if (flags & 0x20) {
      const routeKeyLen = buffer.readUInt8(offset++);
      if (shouldRead("routingKey")) {
        meta.routingKey = buffer.toString("utf8", offset, offset + routeKeyLen);
      }
      offset += routeKeyLen;
    }

    // size
    if (shouldRead("size")) {
      meta.size = buffer.readUInt32BE(offset);
    }
    offset += 4;

    // needAcks
    if (shouldRead("needAcks")) {
      meta.needAcks = buffer.readUInt32BE(offset);
    }

    return meta as Pick<MessageMetadata, K>;
  }

  // Updates specific metadata in the buffer without decoding full message
  updateMetadata(
    buffer: Buffer,
    partialMeta: Partial<MessageMetadata>
  ): Buffer {
    const tempBuffer = Buffer.alloc(buffer.length);
    buffer.copy(tempBuffer);

    let offset = 0;

    if (partialMeta.id !== undefined) {
      tempBuffer.writeUInt32BE(partialMeta.id, offset);
    }
    offset += 4;

    if (partialMeta.ts !== undefined) {
      tempBuffer.writeDoubleBE(partialMeta.ts, offset);
    }
    offset += 8;

    if (partialMeta.producerId !== undefined) {
      tempBuffer.writeUInt32BE(partialMeta.producerId, offset);
    }
    offset += 4;

    const flags = tempBuffer.readUInt8(offset++);
    tempBuffer[offset - 1] = this.updateFlags(flags, partialMeta);

    if (flags & 0x01 && partialMeta.priority !== undefined) {
      tempBuffer.writeUInt8(partialMeta.priority, offset++);
    } else if (flags & 0x01) {
      offset++;
    }

    if (flags & 0x02 && partialMeta.ttl !== undefined) {
      tempBuffer.writeUInt32BE(partialMeta.ttl!, offset);
      offset += 4;
    } else if (flags & 0x02) {
      offset += 4;
    }

    if (flags & 0x04 && partialMeta.ttd !== undefined) {
      tempBuffer.writeUInt32BE(partialMeta.ttd!, offset);
      offset += 4;
    } else if (flags & 0x04) {
      offset += 4;
    }

    if (flags & 0x08 && partialMeta.batchId !== undefined) {
      tempBuffer.writeUInt32BE(partialMeta.batchId!, offset);
      offset += 4;
      tempBuffer.writeUInt16BE(partialMeta.batchIdx!, offset);
      offset += 2;
      tempBuffer.writeUInt16BE(partialMeta.batchSize!, offset);
      offset += 2;
    } else if (flags & 0x08) {
      offset += 8;
    }

    if (partialMeta.attempts !== undefined) {
      tempBuffer.writeUInt8(partialMeta.attempts, offset++);
    } else {
      offset++;
    }

    if (partialMeta.consumedAt !== undefined) {
      tempBuffer.writeDoubleBE(partialMeta.consumedAt, offset);
      offset += 8;
    } else if (flags & 0x10) {
      offset += 8;
    }

    // Skip variable-length fields for now
    // You can enhance this to handle them too if needed

    // Update size or needAcks at the end
    if (partialMeta.size !== undefined) {
      tempBuffer.writeUInt32BE(partialMeta.size, offset);
    }
    offset += 4;

    if (partialMeta.needAcks !== undefined) {
      tempBuffer.writeUInt32BE(partialMeta.needAcks, offset);
    }

    return tempBuffer;
  }

  getMetadataLength(buffer: Buffer): number {
    // Parse flags and calculate how much space was used by metadata
    const flags = buffer[45]; // assuming 45 is offset after initial fixed fields
    let len = 46; // start with fixed size

    if (flags & 0x01) len += 1;
    if (flags & 0x02) len += 4;
    if (flags & 0x04) len += 4;
    if (flags & 0x08) len += 8;
    if (flags & 0x10) len += 1 + buffer[len]; // corrId length prefix
    if (flags & 0x20) len += 1 + buffer[len]; // routingKey length prefix

    len += 4; // size
    len += 4; // needAcks

    return len;
  }

  private updateFlags(flags: number, meta: Partial<MessageMetadata>): number {
    return (
      (meta.priority !== undefined ? 0x01 : flags & ~0x01) |
      (meta.ttl !== undefined ? 0x02 : flags & ~0x02) |
      (meta.ttd !== undefined ? 0x04 : flags & ~0x04) |
      (meta.batchId !== undefined ? 0x08 : flags & ~0x08) |
      (meta.correlationId !== undefined ? 0x10 : flags & ~0x10) |
      (meta.routingKey !== undefined ? 0x20 : flags & ~0x20)
    );
  }
}
