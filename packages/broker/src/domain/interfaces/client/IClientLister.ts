export interface IClientLister {
  list(): Promise<number[]>;
}
