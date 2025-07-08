import type { IMemoryPressureChecker } from "@domain/interfaces/IMemoryPressureChecker";

export class MemoryPressureChecker implements IMemoryPressureChecker {
  static DEFAULT_THRESHOLD_MB = 1024;
  constructor(private thresholdMB?: number) {}

  isPressured(): boolean {
    const { rss } = process.memoryUsage();
    const usedMB = Math.round(rss / 1024 / 1024);
    return (
      usedMB > (this.thresholdMB ?? MemoryPressureChecker.DEFAULT_THRESHOLD_MB)
    );
  }
}
