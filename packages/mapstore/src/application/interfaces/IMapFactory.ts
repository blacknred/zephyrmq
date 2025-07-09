import type { ISerializable } from "@domain/interfaces/ISerializable";
import type { IMap } from "./IMap";

export interface IMapOptions<T> {
  serializer?: ISerializable<T>;
}

export interface IMapFactory {
  create<K extends string | number, V>(
    name: string,
    options?: IMapOptions<V>
  ): IMap<K, V>;
}
