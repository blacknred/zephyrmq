import type { ILogger } from "@domain/interfaces/ILogger";
import type { ILogDriver } from "@domain/interfaces/ILoggerDriver";
import type { ILoggerFactory } from "@domain/interfaces/ILoggerFactory";
import { BufferedLogger } from "./BufferedLogger";

export class BufferLoggerFactory implements ILoggerFactory {
  constructor(
    private driver: ILogDriver,
    private chunkSize = 50
  ) {}
  create(label?: string): ILogger {
    return new BufferedLogger(this.driver, this.chunkSize, label);
  }
}
