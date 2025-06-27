export interface ILogManager<T> {
  log?: T;
  close(): void;
}
