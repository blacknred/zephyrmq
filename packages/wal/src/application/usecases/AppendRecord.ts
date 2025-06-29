import type { IAppender } from "@domain/ports/IAppender";

export class AppendRecord {
  constructor(private appender: IAppender) {}

  async execute(data: Buffer) {
    return this.appender.append(data);
  }
}
