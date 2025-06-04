import crc32 from "crc-32";
import type { IFieldDefinition, ISchema, Schema } from "./types";

export class BinarySchema<T> implements ISchema<T> {
  constructor(private readonly schemaDef: Schema<T>) {}

  serialize(data: T): Buffer {
    const entries = Object.entries(this.schemaDef) as Array<
      [keyof T, IFieldDefinition]
    >;

    let flagByte = 0;
    let flagIndex = 0;

    const fieldValues: Array<{
      key: keyof T;
      def: IFieldDefinition;
      value: any;
    }> = [];

    for (let i = 0; i < entries.length; i++) {
      const [key, def] = entries[i];
      const value = (data as any)[key];

      if (def.optional && value === undefined) continue;

      if (def.optional) {
        flagByte |= 1 << flagIndex;
        flagIndex++;
      }

      fieldValues.push({ key, def, value });
    }

    const buffers: Buffer[] = [];

    if (flagIndex > 0) {
      const flagBuf = Buffer.alloc(1);
      flagBuf.writeUInt8(flagByte, 0);
      buffers.push(flagBuf);
    }

    for (const { def, value } of fieldValues) {
      buffers.push(this.serializeField(def, value));
    }

    const dataBuffer = Buffer.concat(buffers);
    const crcBuffer = this.computeCrc(dataBuffer);

    return Buffer.concat([crcBuffer, dataBuffer]);
  }

  deserialize(buffer: Buffer): T {
    if (buffer.length < 4) {
      throw new Error("Buffer too small for CRC");
    }

    const expectedCrc = buffer.readUInt32BE(0);
    const dataBuffer = buffer.subarray(4);
    const actualCrc = crc32.buf(dataBuffer);

    if (expectedCrc !== actualCrc) {
      throw new Error("CRC32 checksum verification failed");
    }

    const result = {} as T;
    let offset = 0;
    const entries = Object.entries(this.schemaDef) as Array<
      [keyof T, IFieldDefinition]
    >;

    let flagByte = 0;
    let flagIndex = 0;

    const hasOptional = entries.some(([_, def]) => def.optional);

    if (hasOptional) {
      flagByte = dataBuffer.readUInt8(offset++);
    }

    for (let i = 0; i < entries.length; i++) {
      const [key, def] = entries[i];

      if (def.optional && !(flagByte & (1 << flagIndex))) {
        (result as any)[key] = undefined;
        flagIndex++;
        continue;
      }

      if (def.optional) {
        flagIndex++;
      }

      const { value, bytesRead } = this.deserializeField(
        def,
        dataBuffer,
        offset
      );
      (result as any)[key] = value;
      offset += bytesRead;
    }

    return result;
  }

  private computeCrc(buffer: Buffer): Buffer {
    const value = crc32.buf(buffer);
    const crcBuf = Buffer.alloc(4);
    crcBuf.writeUInt32BE(value, 0);
    return crcBuf;
  }

  private serializeField(def: IFieldDefinition, value: any): Buffer {
    switch (def.type) {
      case "int8":
        return Buffer.from([value]);
      case "uint8":
        return Buffer.from([value]);
      case "int16": {
        const buf = Buffer.alloc(2);
        buf.writeInt16BE(value, 0);
        return buf;
      }
      case "uint16": {
        const buf = Buffer.alloc(2);
        buf.writeUInt16BE(value, 0);
        return buf;
      }
      case "int32": {
        const buf = Buffer.alloc(4);
        buf.writeInt32BE(value, 0);
        return buf;
      }
      case "uint32": {
        const buf = Buffer.alloc(4);
        buf.writeUInt32BE(value, 0);
        return buf;
      }
      case "double": {
        const buf = Buffer.alloc(8);
        buf.writeDoubleBE(value, 0);
        return buf;
      }
      case "string": {
        const strBuf = Buffer.from(value, "utf8");
        const lenBuf = Buffer.alloc(2);
        lenBuf.writeUInt16BE(strBuf.length, 0);
        return Buffer.concat([lenBuf, strBuf]);
      }
      case "object": {
        const nestedSchema = new BinarySchema(def.schema!);
        return nestedSchema.serialize(value);
      }
      case "array": {
        const arrLen = Buffer.alloc(2);
        arrLen.writeUInt16BE(value.length, 0);
        const items = value.map((item: any) =>
          this.serializeField(def.itemType!, item)
        );
        return Buffer.concat([arrLen, ...items]);
      }
      default:
        throw new Error(`Unsupported type: ${def.type}`);
    }
  }

  private deserializeField(
    def: IFieldDefinition,
    buffer: Buffer,
    offset: number
  ): { value: any; bytesRead: number } {
    switch (def.type) {
      case "int8":
        return {
          value: buffer.readInt8(offset),
          bytesRead: 1,
        };
      case "uint8":
        return {
          value: buffer.readUInt8(offset),
          bytesRead: 1,
        };
      case "int16":
        return {
          value: buffer.readInt16BE(offset),
          bytesRead: 2,
        };
      case "uint16":
        return {
          value: buffer.readUInt16BE(offset),
          bytesRead: 2,
        };
      case "int32":
        return {
          value: buffer.readInt32BE(offset),
          bytesRead: 4,
        };
      case "uint32":
        return {
          value: buffer.readUInt32BE(offset),
          bytesRead: 4,
        };
      case "double":
        return {
          value: buffer.readDoubleBE(offset),
          bytesRead: 8,
        };
      case "string": {
        const len = buffer.readUInt16BE(offset);
        const value = buffer.toString("utf8", offset + 2, offset + 2 + len);
        return {
          value,
          bytesRead: 2 + len,
        };
      }
      case "object": {
        const schema = new BinarySchema(def.schema!);
        const { value, bytesRead } = schema.deserializeField(
          def,
          buffer,
          offset
        );
        return { value, bytesRead };
      }
      case "array": {
        const len = buffer.readUInt16BE(offset);
        let pos = offset + 2;
        const values = [];
        for (let i = 0; i < len; i++) {
          const { value, bytesRead } = this.deserializeField(
            def.itemType!,
            buffer,
            pos
          );
          values.push(value);
          pos += bytesRead;
        }
        return {
          value: values,
          bytesRead: pos - offset,
        };
      }
      default:
        throw new Error(`Unsupported type: ${def.type}`);
    }
  }
}
