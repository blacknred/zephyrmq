import type { ICacheCapacityCalculator } from "@domain/interfaces/ICacheCapacityCalculator";
import type { IMemoryCapacityService } from "@domain/interfaces/IMemoryCapacityService";

export class CacheCapacityCalculator implements ICacheCapacityCalculator {
  static CACHE_MEMORY_RATIO = 1 / 3;
  static BYTES_PER_ENTRY = 150;

  private cacheCount = 0;
  constructor(
    private memoryCapacityService: IMemoryCapacityService,
    private defaultBytesPerEntry = CacheCapacityCalculator.BYTES_PER_ENTRY,
    private cacheMemoryRatio = CacheCapacityCalculator.CACHE_MEMORY_RATIO
  ) {}

  calculate(bytesPerEntry?: number) {
    const availableBytesPerMap = this.memoryCapacityService.getPartitionedHeap(
      ++this.cacheCount
    );

    const availableBytesForCache = availableBytesPerMap * this.cacheMemoryRatio;

    return Math.floor(
      availableBytesForCache / (bytesPerEntry ?? this.defaultBytesPerEntry)
    );
  }
}
