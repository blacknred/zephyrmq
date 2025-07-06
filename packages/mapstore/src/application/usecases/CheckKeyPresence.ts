import type { IKeyPresenceChecker } from "@domain/ports/IKeyPresenceChecker";

export class CheckKeyPresence<K> {
  constructor(private readonly keyChecker: IKeyPresenceChecker<K>) {}

  execute(key: K) {
    return this.keyChecker.has(key);
  }
}
