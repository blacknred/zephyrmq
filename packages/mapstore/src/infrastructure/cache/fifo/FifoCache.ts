import type { ICache } from "@domain/interfaces/ICache";

export class FifoCache<K, V> extends Map<K, V> implements ICache<K, V> {
  constructor(private maxSize = 10_000) {
    super();
  }

  set(key: K, value: V) {
    super.set(key, value);

    if (this.isFull()) {
      const firstKey = super.keys().next().value!;
      super.delete(firstKey);
    }

    return this;
  }

  isFull(): boolean {
    return this.size >= this.maxSize;
  }
}
