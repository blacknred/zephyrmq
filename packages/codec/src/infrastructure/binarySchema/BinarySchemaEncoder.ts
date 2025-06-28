import msgpack from "@msgpack/msgpack";
import type { CipherGCMTypes } from "node:crypto";
import snappy from "snappy";
import type { IEncoder } from "src/domain/interfaces/IEncoder";
import type { IEncryptionOptions } from "src/domain/interfaces/IEncryptionOptions";
import type { ISchema } from "src/domain/interfaces/ISchema";

export class BinarySchemaEncoder implements IEncoder {
  static readonly COMPRESSION_SIZE_THRESHOLD = 4_000;
  static readonly AUTH_TAG_BYTE_LEN = 16;
  static readonly ALGORITHM: CipherGCMTypes = "aes-256-gcm";
  static readonly IV_LENGTH = 16;

  private readonly key?: Buffer;
  private readonly algorithm: CipherGCMTypes;
  private readonly ivLength: number;

  constructor(
    private compressionSizeThreshold = BinarySchemaEncoder.COMPRESSION_SIZE_THRESHOLD,
    private encryptionOptions?: IEncryptionOptions
  ) {
    this.key = encryptionOptions?.key;
    this.algorithm =
      encryptionOptions?.algorithm ?? BinarySchemaEncoder.ALGORITHM;
    this.ivLength =
      encryptionOptions?.ivLength ?? BinarySchemaEncoder.IV_LENGTH;
  }

  encode<T>(data: T, schema?: ISchema<T>, compress = false): Buffer {
    try {
      let buffer: Buffer;
      if (schema) buffer = schema.serialize(data);
      else if (Buffer.isBuffer(data)) buffer = data;
      else buffer = Buffer.from(msgpack.encode(data));

      return this.compressIfApplicable(buffer, compress);
    } catch (e) {
      throw new Error(`Encoding failed: ${e}`);
    }
  }

  private compressIfApplicable(data: Buffer, compress = false): Buffer {
    if (!compress || data.length < this.compressionSizeThreshold) {
      const header = Buffer.alloc(1);
      header.writeUInt8(0x00, 0); // not compressed
      return Buffer.concat([header, data]);
    }

    const compressed = snappy.compressSync(data);
    const header = Buffer.alloc(1);
    header.writeUInt8(0x01, 0); // compressed
    return Buffer.concat([header, compressed]);
  }

  private encrypt(buffer: Buffer, key: Buffer) {
    const iv = randomBytes(this.ivLength);
    const cipher = createCipheriv(this.algorithm, key, iv);
    const encrypted = Buffer.concat([cipher.update(buffer), cipher.final()]);
    const authTag = cipher.getAuthTag();
    return Buffer.concat([iv, encrypted, authTag]);
  }
}
