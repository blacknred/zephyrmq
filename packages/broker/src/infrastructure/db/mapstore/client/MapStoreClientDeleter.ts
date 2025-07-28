import type { IClientDeleter } from "@domain/interfaces/client/IClientDeleter";
import type { IClientState } from "@domain/interfaces/client/IClientState";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreClientDeleter implements IClientDeleter {
  constructor(private clients: IMap<number, IClientState>) {}

  async delete(id: number) {
    if (!this.clients.has(id)) {
      throw new Error("Client not found");
    }

    this.clients.delete(id);
  }
}
