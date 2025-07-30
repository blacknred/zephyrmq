export interface IMaxUnackedChecker {
  hasExceeded(consumerId: number): Promise<boolean>;
}