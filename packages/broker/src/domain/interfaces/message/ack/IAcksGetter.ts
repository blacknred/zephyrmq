export interface IAcksGetter {
  get(consumerId: number): Promise<Record<number, number>>;
}
