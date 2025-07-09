import type { ILogger } from "@domain/interfaces/ILogger";

export interface ILogService {
  log: ILogger["log"];
  for(name: string): ILogger;
  flushAll(): void;
}
