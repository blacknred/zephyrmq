import type { IMemoryCapacityService } from "@domain/interfaces/IMemoryCapacityService";

export class MemoryCapacityService implements IMemoryCapacityService {
  getAvailableHeap(): number {
    const { heapTotal, heapUsed } = process.memoryUsage();
    return Math.max(0, heapTotal - heapUsed);
  }

  getPartitionedHeap(totalConsumers: number): number {
    return this.getAvailableHeap() / totalConsumers;
  }
}
