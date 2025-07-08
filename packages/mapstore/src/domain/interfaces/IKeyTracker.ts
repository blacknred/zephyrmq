export interface IKeyTracker<T> {
  add(element: T): boolean;
  has(element: T): boolean;
  remove(element: T): boolean;
  clear(): void;
  length: number;
}
