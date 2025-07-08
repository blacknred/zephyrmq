import type { ICache } from "@domain/interfaces/ICache";
import type { IEntriesReader } from "@domain/interfaces/IEntriesReader";
import type { IKeyTracker } from "@domain/interfaces/IKeyTracker";

export class ReadEntries<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly cacheEntriesReader: IEntriesReader<K, V>,
    private readonly dbEntriesReader: IEntriesReader<K, V>,
    private readonly keyTracker: IKeyTracker<K>
  ) {}

  execute() {
    if (this.areAllEntriesInCache()) {
      return this.cacheEntriesReader.entries();
    }

    return this.dbEntriesReader.entries();
  }

  private areAllEntriesInCache(): boolean {
    return this.cache.size === this.keyTracker.length;
  }
}
