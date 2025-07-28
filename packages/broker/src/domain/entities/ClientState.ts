import type { IClientState } from "@domain/interfaces/client/IClientState";
import type { IClientStatus } from "@domain/interfaces/client/IClientStatus";
import type { IClientType } from "@domain/interfaces/client/IClientType";

export class ClientState implements IClientState {
  public registeredAt = Date.now();
  public lastActiveAt = Date.now();
  public status: IClientStatus = "active";
  public messageCount = 0;
  public processingTime = 0;
  public avgProcessingTime = 0;
  public pendingAcks = 0;
  constructor(
    public id: number,
    public clientType: IClientType
  ) {}
}