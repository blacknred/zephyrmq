import type { IHasher } from "@domain/ports/IHasher";
import type { IHashRing } from "@domain/ports/IHashRing";
import type { IMap, IMapStore, ISerializable } from "@zephyrmq/mapstore/index";

class HashToNodeSerializer implements ISerializable<number, number> {
  constructor(
    private nodeIds: Set<number>,
    private sortedHashes: number[]
  ) {}
  serialize(node: number) {
    return node;
  }
  deserialize(node: number, hash: number) {
    this.nodeIds.add(node);
    this.sortedHashes.push(hash);
    this.sortedHashes.sort((a, b) => a - b);
    return node;
  }
}

/** Hash ring.
 * The system works regardless of how different the key hashes are because the lookup is always relative to the fixed node positions on the ring.
 * Sorted nodes in a ring: [**100(A)**, _180(user-123 key hash always belong to the B)_, **200(B)**, **300(A)**, **400(B)**, **500(A)**, **600(B)**]
 */
export class MapStoreHashRing implements IHashRing {
  private hashToNodeMap: IMap<number, number>;
  private sortedHashes: number[] = []; // must be array
  private nodeIds = new Set<number>();

  constructor(
    private hasher: IHasher,
    mapStore: IMapStore,
    label: string,
    private replicas = 3
  ) {
    const serializer = new HashToNodeSerializer(
      this.nodeIds,
      this.sortedHashes
    );
    this.hashToNodeMap = mapStore.createMap<number, number>(
      `hashToNodeMap:${label}`,
      { serializer }
    );
  }

  addNode(id: number): void {
    if (this.nodeIds.has(id)) return;

    for (let i = 0; i < this.replicas; i++) {
      const hash = this.hasher.hash(`${id}-${i}`);
      this.hashToNodeMap.set(hash, id);

      const index = this.findInsertIndex(hash);
      this.sortedHashes.splice(index, 0, hash);
    }

    this.nodeIds.add(id);
  }

  removeNode(id: number): void {
    for (const [nodeId, hash] of this.hashToNodeMap.entries()) {
      if (nodeId !== id) continue;
      this.hashToNodeMap.delete(hash);
      const index = this.sortedHashes.indexOf(hash);
      if (index !== -1) {
        this.sortedHashes.splice(index, 1);
      }
    }

    this.nodeIds.delete(id);
  }

  getNodeCount(): number {
    return this.nodeIds.size;
  }

  *getNode(key: string): Generator<number, void, unknown> {
    if (this.sortedHashes.length === 0) {
      throw new Error("No nodes available in the hash ring");
    }

    const keyHash = this.hasher.hash(key);
    let currentIndex = this.findNodeIndex(keyHash);

    const total = this.sortedHashes.length;
    for (let i = 0; i < total; i++) {
      yield this.hashToNodeMap.get(this.sortedHashes[currentIndex])!;
      currentIndex = (currentIndex + 1) % total;
    }
  }

  private findNodeIndex(keyHash: number): number {
    let low = 0;
    let high = this.sortedHashes.length - 1;

    while (low <= high) {
      const mid = (low + high) >>> 1;
      if (this.sortedHashes[mid] < keyHash) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    return low % this.sortedHashes.length;
  }

  private findInsertIndex(hash: number): number {
    let low = 0;
    let high = this.sortedHashes.length;

    while (low < high) {
      const mid = (low + high) >>> 1;
      if (this.sortedHashes[mid] < hash) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    return low;
  }
}
