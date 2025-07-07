import type { IMapSizer } from "@domain/ports/IMapSizer";

export class LevelDbMapSizer implements IMapSizer {
  private count = 0;

  get size() {
    return this.count;
  }

  set size(n: number) {
    this.count += n;
  }
}
