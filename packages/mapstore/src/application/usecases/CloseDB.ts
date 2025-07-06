import type { IDBCloser } from "@domain/ports/IDBCloser";

export class CloseDB {
  constructor(private readonly dbCloser: IDBCloser) {}

  execute() {
    return this.dbCloser.close();
  }
}
