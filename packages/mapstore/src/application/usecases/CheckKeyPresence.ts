import type { ICache } from "@domain/interfaces/ICache";
import type { IKeyTracker } from "@domain/interfaces/IKeyTracker";

export class CheckKeyPresence<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly keyTracker: IKeyTracker<K>
  ) {}

  execute(key: K) {
    // cache is faster than keytracker so check it first
    if (this.cache.has(key)) return true;
    return this.keyTracker.has(key);
  }
}
