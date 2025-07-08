import type { ICache } from "../../domain/interfaces/ICache";
import type { IKeyTracker } from "../../domain/interfaces/IKeyTracker";
import type { IValuesReader } from "../../domain/interfaces/IValuesReader";

export class ReadValues<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly cacheEntriesReader: IValuesReader<V>,
    private readonly dbEntriesReader: IValuesReader<V>,
    private readonly keyTracker: IKeyTracker<K>
  ) {}

  execute() {
    if (this.areAllValuesInCache()) {
      return this.cacheEntriesReader.values();
    }

    return this.dbEntriesReader.values();
  }

  private areAllValuesInCache(): boolean {
    return this.cache.size === this.keyTracker.length;
  }
}
