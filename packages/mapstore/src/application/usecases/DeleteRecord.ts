import type { IRecordDeleter } from "@domain/ports/IRecordDeleter";

export class DeleteRecord<K> {
  constructor(private readonly recordDeleter: IRecordDeleter<K>) {}

  execute(key: K) {
    return this.recordDeleter.delete(key);
  }
}
