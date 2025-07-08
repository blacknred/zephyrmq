import type { IKeyTracker } from "../../domain/interfaces/IKeyTracker";

export class GetMapSize<K> {
  constructor(private readonly keyTracker: IKeyTracker<K>) {}

  execute() {
    return this.keyTracker.length;
  }
}
