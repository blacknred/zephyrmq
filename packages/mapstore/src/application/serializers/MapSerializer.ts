import type { ISerializable } from "@domain/ports/ISerializable";

export class MapSerializer<K, V> implements ISerializable {
  serialize(map: Map<K, V>): [K, V][] {
    return Array.from(map);
  }
  deserialize(data: [K, V][]): Map<K, V> {
    return new Map<K, V>(data);
  }
}
