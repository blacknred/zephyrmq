import type { TransferListItem } from "worker_threads";

export class TransferableCollector {
  static collect(obj: unknown): TransferListItem[] {
    const transferables: TransferListItem[] = [];

    function walk(val: any) {
      if (val instanceof ArrayBuffer) {
        transferables.push(val);
      } else if (val && typeof val === "object") {
        for (const key in val) {
          walk(val[key]);
        }
      }
    }

    walk(obj);
    return transferables;
  }
}
