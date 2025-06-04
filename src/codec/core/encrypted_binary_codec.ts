import {
  createCipheriv,
  createDecipheriv,
  randomBytes,
  type CipherGCMTypes,
} from "crypto";
import type { ISchema } from "../schema/types";
import { BinaryCodec } from "./binary_codec";
import type { ICodec } from "./types";

export interface IEncryptedCodecOptions {
  key: Buffer; // 32 bytes
  ivLength?: number;
  algorithm?: CipherGCMTypes;
}

export class EncryptedBinaryCodec extends BinaryCodec implements ICodec {
  static ALGORITHM: CipherGCMTypes = "aes-256-gcm";
  static IV_LENGTH = 16;
  static AUTH_TAG_BYTE_LEN = 16;

  private readonly key?: Buffer;
  private readonly algorithm: CipherGCMTypes;
  private readonly ivLength: number;

  constructor(encryptionOptions?: IEncryptedCodecOptions) {
    super();
    this.key = encryptionOptions?.key;
    this.algorithm =
      encryptionOptions?.algorithm ?? EncryptedBinaryCodec.ALGORITHM;
    this.ivLength =
      encryptionOptions?.ivLength ?? EncryptedBinaryCodec.IV_LENGTH;
  }

  override encode<T>(data: T, schema?: ISchema<T>, compress = false): Buffer {
    const buffer = super.encode(data, schema, compress);
    if (!this.key) return buffer;
    return this.encrypt(buffer, this.key);
  }

  override decode<T>(buffer: Buffer, schema?: ISchema<T>): T {
    if (!this.key) return super.decode(buffer, schema);
    const decrypted = this.decrypt(buffer, this.key);
    return super.decode(decrypted, schema);
  }

  private encrypt(buffer: Buffer, key: Buffer) {
    const iv = randomBytes(this.ivLength);
    const cipher = createCipheriv(this.algorithm, key, iv);
    const encrypted = Buffer.concat([cipher.update(buffer), cipher.final()]);
    const authTag = cipher.getAuthTag();
    return Buffer.concat([iv, encrypted, authTag]);
  }

  private decrypt(buffer: Buffer, key: Buffer) {
    if (
      buffer.length <
      this.ivLength + EncryptedBinaryCodec.AUTH_TAG_BYTE_LEN
    ) {
      throw new Error("Buffer too small for IV and auth tag");
    }

    const iv = buffer.subarray(0, this.ivLength);
    const authTag = buffer.subarray(-EncryptedBinaryCodec.AUTH_TAG_BYTE_LEN);
    const encrypted = buffer.subarray(
      this.ivLength,
      buffer.length - EncryptedBinaryCodec.AUTH_TAG_BYTE_LEN
    );

    const decipher = createDecipheriv(this.algorithm, key, iv);
    decipher.setAuthTag(authTag);

    return Buffer.concat([decipher.update(encrypted), decipher.final()]);
  }
}
