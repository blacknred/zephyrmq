import type { CipherGCMTypes } from "node:crypto";

export interface IEncryptionOptions {
  key: Buffer; // 32 bytes
  ivLength?: number;
  algorithm?: CipherGCMTypes;
}
