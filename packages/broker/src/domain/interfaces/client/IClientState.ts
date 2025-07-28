import type { IClientStatus } from "./IClientStatus";
import type { IClientType } from "./IClientType";

export interface IMutableClientState {
  lastActiveAt: number;
  status: IClientStatus;
  messageCount: number;
  processingTime: number;
  avgProcessingTime: number;
  pendingAcks: number;
}

export interface IClientState extends IMutableClientState {
  id: number;
  clientType: IClientType;
  registeredAt: number;
}
