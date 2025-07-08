export interface IReader {
  read(offset: number, length: number): Promise<Buffer | void>;
}
