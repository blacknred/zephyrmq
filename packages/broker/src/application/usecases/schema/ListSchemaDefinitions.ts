import type { IAppender } from "../../domain/interfaces/IAppender";

export class ListSchemaDefinition {
  constructor(private appender: IAppender) {}

  async execute(data: Buffer) {
    return this.appender.append(data);
  }
}