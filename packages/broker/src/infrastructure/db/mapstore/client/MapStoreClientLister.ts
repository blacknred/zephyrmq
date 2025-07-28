import type { IClientLister } from "@domain/interfaces/client/IClientLister";
import type { IClientState } from "@domain/interfaces/client/IClientState";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreClientLister implements IClientLister {
  constructor(private clients: IMap<number, IClientState>) {}

  async list() {
    const ids: number[] = [];
    for await (const name of this.clients.keys()) {
      ids.push(name);
    }

    return ids;
  }
}
