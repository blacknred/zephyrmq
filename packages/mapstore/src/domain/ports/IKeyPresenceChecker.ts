export interface IKeyPresenceChecker<K> {
  has(key: K): boolean;
}
