export interface IDecryptor {
  decrypt(buffer: Buffer): Buffer;
}
