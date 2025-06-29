export interface IMessageRetentionManager {
  start(): void;
  stop(): void;
  markDeletable(id: number): Promise<void>;
  unmarkDeletable(id: number): Promise<void>;
}