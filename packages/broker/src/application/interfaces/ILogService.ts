import type { ILogger } from "@domain/ports/ILogger";

export interface ILogService {
  log: ILogger["log"];
  for(name: string): ILogger;
  flushAll(): void;
}
