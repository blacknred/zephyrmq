export interface IDedupChecker {
  isDeduped(consumerId: number, messageId: number): Promise<boolean>;
}