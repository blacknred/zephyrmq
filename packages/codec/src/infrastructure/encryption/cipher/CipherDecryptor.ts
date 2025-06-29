import { createDecipheriv, type CipherGCMTypes } from "node:crypto";
import type { IDecryptor } from "@domain/ports/IDecryptor";
import type { IEncryptionOptions } from "@domain/ports/IEncryptionOptions";

export class CipherDecryptor implements IDecryptor {
  static readonly AUTH_TAG_BYTE_LEN = 16;
  static readonly ALGORITHM: CipherGCMTypes = "aes-256-gcm";
  static readonly IV_LENGTH = 16;

  private readonly key: Buffer;
  private readonly algorithm: CipherGCMTypes;
  private readonly ivLength: number;

  constructor(encryptionOptions: IEncryptionOptions) {
    this.key = encryptionOptions.key;
    this.algorithm = encryptionOptions.algorithm ?? CipherDecryptor.ALGORITHM;
    this.ivLength = encryptionOptions.ivLength ?? CipherDecryptor.IV_LENGTH;
  }

  decrypt(buffer: Buffer): Buffer {
    if (buffer.length < this.ivLength + CipherDecryptor.AUTH_TAG_BYTE_LEN) {
      throw new Error("Buffer too small for IV and auth tag");
    }

    const iv = buffer.subarray(0, this.ivLength);
    const authTag = buffer.subarray(-CipherDecryptor.AUTH_TAG_BYTE_LEN);
    const encrypted = buffer.subarray(
      this.ivLength,
      buffer.length - CipherDecryptor.AUTH_TAG_BYTE_LEN
    );

    const decipher = createDecipheriv(this.algorithm, this.key, iv);
    decipher.setAuthTag(authTag);

    return Buffer.concat([decipher.update(encrypted), decipher.final()]);
  }
}
