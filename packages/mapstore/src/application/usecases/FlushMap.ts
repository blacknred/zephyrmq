import type { IMapFlusher } from "@domain/ports/IMapFlusher";

export class FlushMap {
  constructor(private readonly mapFlusher: IMapFlusher) {}

  execute() {
    return this.mapFlusher.flush();
  }
}
