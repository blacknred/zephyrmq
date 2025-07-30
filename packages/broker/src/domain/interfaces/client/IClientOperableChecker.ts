export interface IClientOperableChecker {
  isOperable(id: number, now: number): Promise<boolean>;
}