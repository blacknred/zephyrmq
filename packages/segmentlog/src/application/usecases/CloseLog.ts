import type { ISegmentManager } from "@domain/ports/ISegmentManager";

export class CloseLog {
  constructor(private segmentManager: ISegmentManager) {}

  async execute() {
    return this.segmentManager.close();
  }
}
