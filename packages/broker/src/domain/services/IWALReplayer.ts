export interface IWALReplayer {
  replay(): Promise<void>;
}