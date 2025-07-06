import type { ISerializable } from "@domain/ports/ISerializable";
import type { IMap } from "./IMap";

export interface IMapOptions<T> {
  maxSize?: number;
  serializer?: ISerializable<T>;
}

export interface IMapFactory {
  create<K extends string | number, V>(
    name: string,
    options?: IMapOptions<V>
  ): IMap<K, V>;
}
