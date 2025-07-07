// import type { IDBFlushManager } from "@domain/ports/IDBFlushManager";
// import { Mutex } from "@infra/util/Mutex";
// import type { Level } from "level";

// export abstract class LevelDbStructure {
//   protected mutex = new Mutex();
//   protected isCleared = false;
//   protected count = 0;

//   constructor(
//     protected db: Level<string, unknown>,
//     protected flushManager: IDBFlushManager,
//     protected name: string
//   ) {
//     this.init();
//   }

//   // management

//   protected abstract restoreItem(
//     key: string | number,
//     value: unknown
//   ): Promise<void>;

//   protected async init() {
//     this.flushManager.register(this.flush);

//     try {
//       for await (const [k, v] of this.db.iterator({
//         gt: `${this.name}!`,
//         lt: `${this.name}~`,
//       })) {
//         const key = k.slice(this.name.length + 1);
//         await this.restoreItem(key, v);
//       }
//     } catch (cause) {
//       throw new Error(`Failed to restore ${this.name}`, { cause });
//     }
//   }

//   close(): void {
//     this.flushManager.unregister(this.flush);
//   }

//   protected abstract evictIfFull(): void;

//   // flush

//   protected abstract isFlushSkipped(): boolean;
//   protected abstract flushCleanup(): void;
//   protected abstract flushIterator(): Generator<
//     readonly [string | number, any],
//     void,
//     unknown
//   >;
//   flush = async () => {
//     if (this.isFlushSkipped()) return;
//     await this.mutex.acquire();

//     try {
//       if (this.isCleared) {
//         await this.db.clear({
//           gt: `${this.name}!`,
//           lt: `${this.name}~`,
//         });

//         this.count = 0;
//         this.isCleared = false;
//         this.flushCleanup();
//         return;
//       }

//       const batch = this.db.batch();

//       for (const [key, value] of this.flushIterator()) {
//         const prefix = `${this.name}!${key}`;
//         if (value !== undefined) {
//           this.count++;
//           batch.put(prefix, value);
//         } else {
//           this.count--;
//           batch.del(prefix);
//         }
//       }

//       await batch.write();
//       this.flushCleanup();
//     } catch (cause) {
//       throw new Error(`Failed to flush ${this.name}`, { cause });
//     } finally {
//       this.mutex.release();
//     }
//   };
// }
