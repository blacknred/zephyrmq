import type { ILogger } from "@domain/ports/ILogger";
import type { ILogDriver } from "@domain/ports/ILoggerDriver";
import type { ILoggerFactory } from "@domain/ports/ILoggerFactory";
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
