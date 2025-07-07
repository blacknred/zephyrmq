export interface IHashRing {
  addNode(id: number): void;
  removeNode(id: number): void;
  getNodeCount(): number;
  getNode(key: string): Generator<number, void, unknown>;
}