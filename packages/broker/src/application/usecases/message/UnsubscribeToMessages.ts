import type { IAppender } from "../../domain/interfaces/IAppender";

export class UnsubscribeToMessages {
  constructor(private appender: IAppender) {}

  async execute(data: Buffer) {
    return this.appender.append(data);
  }
}
