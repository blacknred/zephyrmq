export class TransferableDetector {
  static isTransferable(value: unknown): boolean {
    if (!value || typeof value !== "object") return false;
    if (value instanceof ArrayBuffer) return true;

    for (const key in value as Record<string, unknown>) {
      if (this.isTransferable((value as any)[key])) return true;
    }

    return false;
  }
}
