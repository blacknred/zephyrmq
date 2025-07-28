import type { IClientState } from "./IClientState";
import type { IClientType } from "./IClientType";

export interface IClientCreator {
  create(type: IClientType): Promise<IClientState>;
}
