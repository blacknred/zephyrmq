import type { IMapCleaner } from "@domain/ports/IMapCleaner";

export class CleanMap {
  constructor(private readonly mapCleaner: IMapCleaner) {}

  execute() {
    return this.mapCleaner.clean();
  }
}
