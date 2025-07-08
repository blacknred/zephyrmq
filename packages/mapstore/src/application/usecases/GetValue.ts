import type { ICache } from "../../domain/interfaces/ICache";
import type { IKeyTracker } from "../../domain/interfaces/IKeyTracker";
import type { IValueGetter } from "../../domain/interfaces/IValueGetter";

export class GetValue<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly dbValueGetter: IValueGetter<K, V>,
    private readonly keyTracker: IKeyTracker<K>
  ) {}

  async execute(key: K) {
    // cache is faster than keytracker so check it first
    let value = this.cache.get(key);
    if (value != undefined) return value;

    if (!this.keyTracker.has(key)) {
      return undefined;
    }

    value = await this.dbValueGetter.get(key);
    if (value != undefined) {
      this.cache.set(key, value);
    }

    return value;
  }
}
