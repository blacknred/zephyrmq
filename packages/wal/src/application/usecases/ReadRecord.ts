import type { IReader } from "../../domain/ports/IReader";

export class ReadRecord {
  constructor(private reader: IReader) {}

  async execute(offset: number, length: number) {
    return this.reader.read(offset, length);
  }
}
