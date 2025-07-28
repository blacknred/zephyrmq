import type { IClientGetter } from "@domain/interfaces/client/IClientGetter";
import type { IClientState } from "@domain/interfaces/client/IClientState";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreClientGetter implements IClientGetter {
  constructor(private clients: IMap<number, IClientState>) {}

  async get(id: number) {
    return this.clients.get(id);
  }
}
