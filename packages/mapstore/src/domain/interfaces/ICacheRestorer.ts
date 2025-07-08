export interface ICacheRestorer {
  restore(): Promise<void>;
}
