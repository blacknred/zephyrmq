import type { IClientCreator } from "@domain/interfaces/client/IClientCreator";
import type { IClientDeleter } from "@domain/interfaces/client/IClientDeleter";
import type { IClientGetter } from "@domain/interfaces/client/IClientGetter";
import type { IClientLister } from "@domain/interfaces/client/IClientLister";

export interface IClientRegistry
  extends IClientCreator,
    IClientDeleter,
    IClientGetter,
    IClientLister {}
