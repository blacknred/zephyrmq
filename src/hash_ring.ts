import crypto from "node:crypto";
import type { IPersistedMap, IPersistedMapFactory, ISerializable } from ".";

export interface IHashService {
  hash(key: string): number;
}

export class SHA256HashService implements IHashService {
  hash(key: string): number {
    const hex = crypto.createHash("sha256").update(key).digest("hex");
    return parseInt(hex.slice(0, 8), 16);
  }
}

export interface IHashRing extends ISerializable {
  addNode(id: number): void;
  removeNode(id: number): void;
  getNodeCount(): number;
  getNode(key: string): Generator<number, void, unknown>;
}

/** Hash ring.
 * The system works regardless of how different the key hashes are because the lookup is always relative to the fixed node positions on the ring.
 * Sorted nodes in a ring: [**100(A)**, _180(user-123 key hash always belong to the B)_, **200(B)**, **300(A)**, **400(B)**, **500(A)**, **600(B)**]
 */
export class InMemoryHashRing implements IHashRing {
  private hashToNodeMap: IPersistedMap<number, number>;
  private sortedHashes: number[] = []; // must be array
  private nodeIds = new Set<number>();

  constructor(
    private hashService: IHashService,
    mapFactory: IPersistedMapFactory,
    label: string,
    private replicas = 3
  ) {
    this.hashToNodeMap = mapFactory.create<number, number>(
      `hashToNodeMap:${label}`,
      this
    );
  }

  serialize(node: number): number {
    return node;
  }

  deserialize(node: number, hash: number): number {
    this.nodeIds.add(node);
    this.sortedHashes.push(hash);
    this.sortedHashes.sort((a, b) => a - b);
    return node;
  }

  addNode(id: number): void {
    if (this.nodeIds.has(id)) return;

    for (let i = 0; i < this.replicas; i++) {
      const hash = this.hashService.hash(`${id}-${i}`);
      this.hashToNodeMap.set(hash, id);

      const index = this.findInsertIndex(hash);
      this.sortedHashes.splice(index, 0, hash);
    }

    this.nodeIds.add(id);
  }

  removeNode(id: number): void {
    const hashesToRemove: number[] = [];
    this.hashToNodeMap.forEach((nodeId, hash) => {
      if (nodeId === id) {
        hashesToRemove.push(hash);
      }
    });

    for (const hash of hashesToRemove) {
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

    const keyHash = this.hashService.hash(key);
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
