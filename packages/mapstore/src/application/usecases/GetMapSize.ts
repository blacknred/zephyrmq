import type { IMapSizer } from "@domain/ports/IMapSizer";

export class GetMapSize {
  constructor(private readonly mapSizer: IMapSizer) {}

  execute() {
    return this.mapSizer.size;
  }
}
