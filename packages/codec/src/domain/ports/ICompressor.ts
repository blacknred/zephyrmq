export interface ICompressor {
  compress(data: Buffer, needCompress: boolean): Buffer;
}
