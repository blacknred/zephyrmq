export interface ICacheCapacityCalculator {
  calculate(bytesPerEntry?: number): number;
}
