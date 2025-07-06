import type { IValueGetter } from "@domain/ports/IValueGetter";

export class GetValue<K, V> {
  constructor(private readonly valueGetter: IValueGetter<K, V>) {}

  execute(key: K) {
    return this.valueGetter.get(key);
  }
}
