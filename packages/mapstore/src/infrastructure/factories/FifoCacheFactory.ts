import type { ICacheCapacityCalculator } from "@domain/interfaces/ICacheCapacityCalculator";
import { FifoCache } from "@infra/cache/fifo/FifoCache";
import { FifoCacheEntriesReader } from "@infra/cache/fifo/FifoCacheEntriesReader";
import { FifoCacheKeysReader } from "@infra/cache/fifo/FifoCacheKeysReader";
import { FifoCacheValuesReader } from "@infra/cache/fifo/FifoCacheValuesReader";
import type { ICacheFactory } from "./ICacheFactory";

export class FifoCacheFactory implements ICacheFactory {
  constructor(private capacityCalculator: ICacheCapacityCalculator) {}

  create<K extends string | number, V>(bytesPerEntry?: number) {
    const maxSize = this.capacityCalculator.calculate(bytesPerEntry);

    const cache = new FifoCache<K, V>(maxSize);
    const cacheEnriesReader = new FifoCacheEntriesReader(cache);
    const cacheKeysReader = new FifoCacheKeysReader(cache);
    const cacheValuesReader = new FifoCacheValuesReader(cache);

    return {
      cache,
      cacheEnriesReader,
      cacheKeysReader,
      cacheValuesReader,
    };
  }
}
