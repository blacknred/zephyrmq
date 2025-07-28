import { ClientState } from "@domain/entities/ClientState";
import type { IClientCreator } from "@domain/interfaces/client/IClientCreator";
import type { IClientState } from "@domain/interfaces/client/IClientState";
import type { IClientType } from "@domain/interfaces/client/IClientType";
import { uniqueIntGenerator } from "@infra/utils";
import type { IMap } from "@zephyrmq/mapstore";

export class MapStoreClientCreator implements IClientCreator {
  constructor(private clients: IMap<number, IClientState>) {}

  async create(type: IClientType) {
    const id = uniqueIntGenerator();

    const state = new ClientState(id, type);
    this.clients.set(id, state);
    return state;
  }
}
