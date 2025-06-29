import type { ITruncator } from "@domain/ports/ITruncator";

export class TruncateRecords {
  constructor(private truncator: ITruncator) {}

  async execute(upToOffset: number) {
    return this.truncator.truncate(upToOffset);
  }
}
