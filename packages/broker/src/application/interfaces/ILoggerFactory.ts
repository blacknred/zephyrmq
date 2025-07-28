import type { ILogger } from "./ILogger";

export interface ILoggerFactory {
  create(label?: string): ILogger;
}
