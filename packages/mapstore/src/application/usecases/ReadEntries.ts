import type { IEntriesReader } from "@domain/ports/IEntriesReader";

export class ReadEntries<K, V> {
  constructor(private readonly entriesReader: IEntriesReader<K, V>) {}

  execute() {
    return this.entriesReader.entries();
  }
}
