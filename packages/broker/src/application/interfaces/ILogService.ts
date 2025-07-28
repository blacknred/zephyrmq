import type { ILogger } from "./ILogger";

export interface ILogService {
  log: ILogger["log"];
  for(name: string): ILogger;
  flushAll(): void;
}
