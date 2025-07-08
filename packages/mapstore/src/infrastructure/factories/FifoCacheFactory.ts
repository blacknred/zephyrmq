import type { ICacheFactory } from "@infra/factories/ICacheFactory";
import type { IMemoryCapacityService } from "@domain/interfaces/IMemoryCapacityService";
import { FifoCache } from "@infra/cache/fifo/FifoCache";
import { FifoCacheEntriesReader } from "@infra/cache/fifo/FifoCacheEntriesReader";
import { FifoCacheKeysReader } from "@infra/cache/fifo/FifoCacheKeysReader";
import { FifoCacheValuesReader } from "@infra/cache/fifo/FifoCacheValuesReader";

export class FifoCacheFactory implements ICacheFactory {
  static CACHE_MEMORY_RATIO = 1 / 3;
  static DEFAULT_BYTES_PER_ENTRY = 150;

  private cacheCount = 0;

  constructor(private memoryCapacityService: IMemoryCapacityService) {}

  create<K extends string | number, V>(bytesPerEntry?: number) {
    const availableBytesPerMap = this.memoryCapacityService.getPartitionedHeap(
      ++this.cacheCount
    );

    const availableBytesForCache =
      availableBytesPerMap * FifoCacheFactory.CACHE_MEMORY_RATIO;

    const maxSize = Math.floor(
      availableBytesForCache /
        (bytesPerEntry ?? FifoCacheFactory.DEFAULT_BYTES_PER_ENTRY)
    );

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
