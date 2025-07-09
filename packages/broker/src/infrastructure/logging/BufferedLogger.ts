import type { ILogger } from "@domain/interfaces/ILogger";
import type { ILogDriver } from "@domain/interfaces/ILoggerDriver";

export class BufferedLogger implements ILogger {
  private flushId?: NodeJS.Immediate;
  private buffer: Array<[string, object, keyof ILogDriver]> = [];

  constructor(
    private logger: ILogDriver,
    private chunkSize = 50,
    private label?: string
  ) {}

  log(msg: string, extra?: object, level: keyof ILogDriver = "info") {
    this.buffer.push([msg, extra ?? {}, level]);
    this.scheduleFlush();
  }

  private scheduleFlush() {
    this.flushId ??= setImmediate(this.flush);
  }

  flush = () => {
    this.flushId = undefined;
    let count = 0;
    const ts = Date.now();
    const { label } = this;

    while (this.buffer.length > 0) {
      if (count++ >= this.chunkSize) break;
      const [message, extra, level] = this.buffer.shift()!;
      this.logger[level]?.(message, Object.assign(extra, { label, ts }));
    }

    if (this.buffer.length > 0) {
      this.scheduleFlush();
    }
  };

  destroy() {
    clearImmediate(this.flushId);
    this.flushId = undefined;
    this.buffer = [];
  }
}
