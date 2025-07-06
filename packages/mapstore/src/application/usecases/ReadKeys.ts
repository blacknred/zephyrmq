import type { IKeysReader } from "@domain/ports/IKeysReader";

export class ReadKeys<K> {
  constructor(private readonly keysReader: IKeysReader<K>) {}

  execute() {
    return this.keysReader.keys();
  }
}
