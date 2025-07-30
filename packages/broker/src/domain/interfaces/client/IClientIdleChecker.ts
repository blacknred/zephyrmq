export interface IClientIdleChecker {
  isIdle(id: number): Promise<boolean>;
}
