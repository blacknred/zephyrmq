import type { ICache } from "@domain/ports/ICache";

export class FifoCache<K, V> extends Map<K, V> implements ICache<K, V> {
  private maxSize = 5000;

  constructor(id: number, bytesPerEntry = 150) {
    super();
    this.countMaxSize(id, bytesPerEntry);
  }

  private countMaxSize(totalMapsCount: number, bytesPerEntry: number) {
    const { heapTotal, heapUsed } = process.memoryUsage();
    const availableBytes = heapTotal - heapUsed;
    if (availableBytes <= 0) return;

    const bytesPerMap = availableBytes / totalMapsCount;
    const maxEntriesPerMap = Math.floor(bytesPerMap / bytesPerEntry);
    this.maxSize = Math.min(this.maxSize, maxEntriesPerMap);
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
