import { createCipheriv, randomBytes, type CipherGCMTypes } from "node:crypto";
import type { IEncryptionOptions } from "@domain/interfaces/IEncryptionOptions";
import type { IEncryptor } from "@domain/interfaces/IEncryptor";

export class CipherEncryptor implements IEncryptor {
  static readonly AUTH_TAG_BYTE_LEN = 16;
  static readonly ALGORITHM: CipherGCMTypes = "aes-256-gcm";
  static readonly IV_LENGTH = 16;

  private readonly key: Buffer;
  private readonly algorithm: CipherGCMTypes;
  private readonly ivLength: number;

  constructor(encryptionOptions: IEncryptionOptions) {
    this.key = encryptionOptions.key;
    this.algorithm = encryptionOptions.algorithm ?? CipherEncryptor.ALGORITHM;
    this.ivLength = encryptionOptions.ivLength ?? CipherEncryptor.IV_LENGTH;
  }

  encrypt(buffer: Buffer): Buffer {
    const iv = randomBytes(this.ivLength);
    const cipher = createCipheriv(this.algorithm, this.key, iv);
    const encrypted = Buffer.concat([cipher.update(buffer), cipher.final()]);
    const authTag = cipher.getAuthTag();
    return Buffer.concat([iv, encrypted, authTag]);
  }
}
