import type { IMapStore } from "./interfaces/IDB";
import type { IMapOptions } from "./interfaces/IMapFactory";
import type { CloseDB } from "./usecases/CloseDB";
import type { CreateMap } from "./usecases/CreateMap";

export class MapStore implements IMapStore {
  constructor(
    private mapCreator: CreateMap,
    private closer: CloseDB
  ) {}

  createMap<K extends string | number, V>(
    name: string,
    options?: IMapOptions<V>
  ) {
    return this.mapCreator.execute<K, V>(name, options);
  }

  async close(): Promise<void> {
    return this.closer.execute();
  }
}
