export interface ISubscriptionChecker {
  isSubscribed(consumerId: number): Promise<boolean>;
}