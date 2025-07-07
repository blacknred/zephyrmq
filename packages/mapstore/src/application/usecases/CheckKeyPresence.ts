import type { ICache } from "@domain/ports/ICache";
import type { IValueGetter } from "@domain/ports/IValueGetter";

export class CheckKeyPresence<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly valueGetter: IValueGetter<K, V>
  ) {}

  async execute(key: K) {
    const exists = this.cache.has(key);
    if (!exists) {
      const value = await this.valueGetter.get(key);
      if (value) this.cache.set(key, value);
      return value != undefined;
    }
    return exists;
  }
}
