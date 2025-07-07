// import type { IDBFlushManager } from "@domain/ports/IDBFlushManager";
// import type { IMap } from "@domain/ports/structs/IMap";
// import type { ISerializable } from "@domain/ports/ISerializable";
// import type { Level } from "level";
// import { LevelDbStructure } from "./LevelDbStructure";

// export class LevelDbMap<K extends string | number, V>
//   extends LevelDbStructure
//   implements IMap<K, V>
// {
//   private map = new Map<K, V>();
//   private dirtyKeys = new Set<K>();

//   private maxSize = Infinity;
//   private serializer?: ISerializable<V>;

//   constructor(
//     db: Level<string, unknown>,
//     flushManager: IDBFlushManager,
//     name: string,
//     maxSize?: number,
//     serializer?: ISerializable<V>
//   ) {
//     super(db, flushManager, name);
//     if (maxSize) this.maxSize = maxSize;
//     if (serializer) this.serializer = serializer;
//   }

//   has(key: K) {
//     return this.map.has(key);
//   }

//   get(key: K) {
//     const value = this.map.get(key);
//     return value ?? (this.db.getSync(`${key}`) as V);
//   }

//   set(key: K, value: V) {
//     // works for primitives, non-primitive will always return true which is also ok
//     if (this.map.get(key) !== value) {
//       this.dirtyKeys.add(key);
//       this.flushManager.commit();
//     }
//     this.map.set(key, value);
//     this.evictIfFull();
//     return this.map;
//   }

//   delete(key: K): boolean {
//     this.dirtyKeys.add(key);
//     this.flushManager.commit();
//     return this.map.delete(key);
//   }

//   clear(): void {
//     this.isCleared = true;
//     this.map.clear();
//     this.flush();
//   }

//   get size(): number {
//     return this.count;
//   }

//   keys() {
//     return this.map.keys();
//   }

//   values() {
//     return this.map.values();
//   }

//   entries() {
//     return this.map.entries();
//   }

//   override evictIfFull(): void {
//     // TODO: LRU-style eviction (simplified)
//     if (this.map.size >= this.maxSize) {
//       const firstKey = this.map.keys().next().value!;
//       this.map.delete(firstKey);
//     }
//   }

//   override async restoreItem(key: K, value: V): Promise<void> {
//     const deserialisedValue =
//       this.serializer?.deserialize(value, key as K) ?? value;
//     this.map.set(key, deserialisedValue);
//   }

//   override isFlushSkipped(): boolean {
//     return this.dirtyKeys.size === 0;
//   }

//   override flushCleanup() {
//     this.dirtyKeys.clear();
//   }

//   override *flushIterator() {
//     for (const key of this.dirtyKeys) {
//       const value = this.map.get(key);
//       if (value == undefined) {
//         yield [key, undefined] as const;
//       } else {
//         const v = this.serializer?.serialize(value, key) ?? value;
//         yield [key, v] as const;
//       }
//     }
//   }
// }
