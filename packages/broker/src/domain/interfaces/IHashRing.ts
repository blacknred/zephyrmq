export interface IHashRing {
  addNode(id: number): Promise<void>;
  removeNode(id: number): Promise<void>;
  getNodeCount(): number;
  getNodes(key: string): AsyncGenerator<number, void, unknown>;
}