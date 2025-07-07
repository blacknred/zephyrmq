import type { ICache } from "@domain/ports/ICache";
import type { IValueGetter } from "@domain/ports/IValueGetter";

export class GetValue<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly valueGetter: IValueGetter<K, V>
  ) {}

  async execute(key: K) {
    let value = this.cache.get(key);
    if (value != undefined) return value;
    value = await this.valueGetter.get(key);
    if (value != undefined) {
      this.cache.set(key, value);
    }
    return value;
  }
}
