import type {
  IClientState,
  IMutableClientState,
} from "@domain/interfaces/client/IClientState";
import type { IClientUpdater } from "@domain/interfaces/client/IClientUpdater";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreClientUpdater implements IClientUpdater {
  constructor(private clients: IMap<number, IClientState>) {}

  async update(id: number, state: IMutableClientState) {
    const client = await this.clients.get(id);
    if (!client) {
      throw new Error("Client not found");
    }

    const updatedClient = Object.assign(client, state);
    updatedClient.lastActiveAt = Date.now();
    this.clients.set(id, updatedClient);
    return updatedClient;
  }
}
