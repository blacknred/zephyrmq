import type { ICache } from "@domain/interfaces/ICache";
import type { ICleaner } from "@domain/interfaces/ICleaner";
import type { IKeyTracker } from "@domain/interfaces/IKeyTracker";

export class CleanMap<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly dbCleaner: ICleaner,
    private readonly keyTracker: IKeyTracker<K>
  ) {}

  execute() {
    this.cache.clear();
    this.dbCleaner.clear();
    this.keyTracker.clear();
  }
}
