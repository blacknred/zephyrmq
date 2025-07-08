import type { IMap } from "./IMap";
import type { IMapOptions } from "./IMapFactory";

export interface IMapStore {
  close(): Promise<void>;
  createMap<K extends string | number, V>(
    name: string,
    options?: IMapOptions<V>
  ): IMap<K, V>;
}
