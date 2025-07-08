import type { ICache } from "@domain/interfaces/ICache";
import type { IKeyTracker } from "@domain/interfaces/IKeyTracker";
import type { IKeysReader } from "@domain/interfaces/IKeysReader";

export class ReadKeys<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly cacheEntriesReader: IKeysReader<K>,
    private readonly dbEntriesReader: IKeysReader<K>,
    private readonly keyTracker: IKeyTracker<K>
  ) {}

  execute() {
    if (this.areAllKeysInCache()) {
      return this.cacheEntriesReader.keys();
    }

    return this.dbEntriesReader.keys();
  }

  private areAllKeysInCache(): boolean {
    return this.cache.size === this.keyTracker.length;
  }
}
