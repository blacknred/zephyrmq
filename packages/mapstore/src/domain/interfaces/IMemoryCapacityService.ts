export interface IMemoryCapacityService {
  getAvailableHeap(): number;
  getPartitionedHeap(totalConsumers: number): number;
}