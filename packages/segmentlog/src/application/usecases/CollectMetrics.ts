import type { ISegmentManager } from "../../domain/interfaces/ISegmentManager";

export class CollectMetrics {
  constructor(private segmentManager: ISegmentManager) {}

  async execute() {
    const currentSegmentId = this.segmentManager.getCurrentSegment()?.id;
    const segments = this.segmentManager.getAllSegments();
    const [totalSize, recordCount] = segments.reduce(
      (sum, seg) => {
        sum[0] += seg.size;
        sum[1] += seg.recordCount;
        return sum;
      },
      [0, 0]
    );

    return {
      totalSize,
      recordCount,
      currentSegmentId,
      segmentCount: segments.length,
    };
  }
}
