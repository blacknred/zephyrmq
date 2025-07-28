import type { IHasher } from "@domain/interfaces/IHasher";
import type { IHashRing } from "@domain/interfaces/IHashRing";

// class HashToNodeSerializer implements ISerializable<number, number> {
//   constructor(
//     private nodeIds: Set<number>,
//     private sortedHashes: number[]
//   ) {}
//   serialize(node: number) {
//     return node;
//   }
//   deserialize(node: number, hash: number) {
//     this.nodeIds.add(node);
//     this.sortedHashes.push(hash);
//     this.sortedHashes.sort((a, b) => a - b);
//     return node;
//   }
// }

// export class MapStoreHashRingFactory {
//   constructor(private mapStore: IMapStore) {}

//   create(label: string, replicas?: number) {
//     const hasher = new SHA256Hasher();
//     const serializer = new HashToNodeSerializer(
//       this.nodeIds,
//       this.sortedHashes
//     );

//     const hashToNodeMap = this.mapStore.createMap<number, number>(
//       `hashToNodeMap:${label}`,
//       { serializer }
//     );

//     return new HashRing(hasher, hashToNodeMap, replicas);
//   }
// }

/** Hash ring.
 * The system works regardless of how different the key hashes are because the lookup is always relative to the fixed node positions on the ring.
 * Sorted nodes in a ring: [**100(A)**, _180(user-123 key hash always belong to the B)_, **200(B)**, **300(A)**, **400(B)**, **500(A)**, **600(B)**]
 */
interface IMap<K, V>
  extends Pick<Map<K, V>, "set" | "get" | "delete" | "entries"> {}

export class HashRing implements IHashRing {
  private sortedHashes: number[] = []; // must be array
  private nodeIds = new Set<number>();

  constructor(
    private hasher: IHasher,
    private hashToNodeMap: IMap<number, number>,
    private replicas = 3
  ) {}

  async addNode(id: number): Promise<void> {
    if (this.nodeIds.has(id)) return;

    for (let i = 0; i < this.replicas; i++) {
      const hash = this.hasher.hash(`${id}-${i}`);
      this.hashToNodeMap.set(hash, id);

      const index = this.findInsertIndex(hash);
      this.sortedHashes.splice(index, 0, hash);
    }

    this.nodeIds.add(id);
  }

  async removeNode(id: number): Promise<void> {
    for await (const [nodeId, hash] of this.hashToNodeMap.entries()) {
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

  async *getNodes(key: string): AsyncGenerator<number, void, unknown> {
    if (this.sortedHashes.length === 0) {
      throw new Error("No nodes available in the hash ring");
    }

    const keyHash = this.hasher.hash(key);
    let currentIndex = this.findNodeIndex(keyHash);

    const total = this.sortedHashes.length;
    for (let i = 0; i < total; i++) {
      const node = await this.hashToNodeMap.get(
        this.sortedHashes[currentIndex]
      );

      if (node) yield node;
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
