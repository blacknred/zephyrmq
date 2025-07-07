import type { IPriorityQueue } from "@domain/ports/IPriorityQueue";
import type { IMap, IMapStore, ISerializable } from "@zephyrmq/mapstore/index";

export class MapStorePriorityQueue<T> implements IPriorityQueue<T> {
  private pendingUpdates: IMap<string, [T, number] | undefined>;

  constructor(
    private queue: IPriorityQueue<T>,
    mapStore: IMapStore,
    name: string,
    maxSize?: number,
    serializer?: ISerializable<[T, number] | undefined>,
    private keyRetriever?: (entry: T) => string
  ) {
    this.pendingUpdates = mapStore.createMap<string, [T, number] | undefined>(
      name,
      {
        maxSize,
        serializer,
      }
    );
  }

  enqueue(value: T, priority = 0): void {
    this.queue.enqueue(value, priority);
    const key = this.keyRetriever?.(value) ?? String(value);
    this.pendingUpdates.set(key, [value, priority]);
    this.pendingUpdates.flush();
  }

  dequeue(): T | undefined {
    const data = this.queue.dequeue();
    if (data) {
      const key = this.keyRetriever?.(data) ?? String(data);
      this.pendingUpdates.set(key, undefined);
      this.pendingUpdates.flush();
    }

    return data;
  }

  isEmpty(): boolean {
    return this.queue.isEmpty();
  }

  peek(): T | undefined {
    return this.queue.peek();
  }

  size(): number {
    return this.queue.size();
  }
}
