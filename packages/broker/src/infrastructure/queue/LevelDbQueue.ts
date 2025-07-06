import type { IFlushManager } from "@domain/ports/IFlushManager";
import type { IQueue } from "@domain/ports/structs/IQueue";
import type { ISerializable } from "@domain/ports/ISerializable";
import { BinaryHeapPriorityQueue } from "./BinaryHeapPriorityQueue";
import type { Level } from "level";
import { LevelDbStructure } from "../../../../mapstore/src/infrastructure/leveldb/_struct/LevelDbStructure";

export class LevelDbQueue<T> extends LevelDbStructure implements IQueue<T> {
  private queue = new BinaryHeapPriorityQueue();
  private pendingUpdates = new Map<string, [T, number] | undefined>();
  private maxSize = Infinity;
  private keyRetriever?: (entry: T) => string;
  private serializer?: ISerializable<T>;

  constructor(
    db: Level<string, unknown>,
    flushManager: IFlushManager,
    name: string,
    maxSize?: number,
    keyRetriever?: (entry: T) => string,
    serializer?: ISerializable<T>
  ) {
    super(db, flushManager, name);
    if (maxSize) this.maxSize = maxSize;
    if (keyRetriever) this.keyRetriever = keyRetriever;
    if (serializer) this.serializer = serializer;
  }

  enqueue(value: T, priority = 0): void {
    this.queue.enqueue(value, priority);
    const key = this.keyRetriever?.(value) ?? String(value);
    this.pendingUpdates.set(key, [value, priority]);
    this.flushManager?.commit();
    this.evictIfFull();
  }

  dequeue(): T | undefined {
    const data = this.queue.dequeue();
    if (data) {
      const key = this.keyRetriever?.(data) ?? String(data);
      this.pendingUpdates.set(key, undefined);
      this.flushManager?.commit();
    }

    return data;
  }

  clear() {
    this.isCleared = true;
    this.flush();
  }

  size(): number {
    return this.queue.size();
  }

  peek() {
    return this.queue.peek();
  }

  isEmpty(): boolean {
    return this.queue.isEmpty();
  }

  override evictIfFull(): void {
    // TODO: LRU-style eviction (simplified)
    if (this.queue.size() < this.maxSize) return;
    this.dequeue();
  }

  override async restoreItem(key: string, value: [T, number]): Promise<void> {
    try {
      const [rawData, priority] = value;
      this.queue.enqueue(
        this.serializer?.deserialize(rawData, key) ?? rawData,
        priority
      );
    } catch (e) {}
  }

  override isFlushSkipped(): boolean {
    return this.pendingUpdates.size === 0;
  }

  override flushCleanup() {
    this.pendingUpdates.clear();
  }

  override *flushIterator() {
    for (const [key, value] of this.pendingUpdates) {
      if (value == undefined) {
        yield [key, undefined] as const;
      } else {
        const [data, priority] = value;
        yield [
          key,
          [this.serializer?.serialize(data, key) ?? data, priority],
        ] as const;
      }
    }
  }
}
