import type { IDBFlusher } from "@domain/interfaces/IDBFlusher";

export class FlushMap<K, V> {
  constructor(private readonly dbFlusher: IDBFlusher<K, V>) {}

  execute() {
    return this.dbFlusher.flush();
  }
}
