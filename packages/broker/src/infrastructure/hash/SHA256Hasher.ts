import type { IHasher } from "@domain/ports/IHasher";
import crypto from "node:crypto";

export class SHA256Hasher implements IHasher {
  hash(key: string): number {
    const hex = crypto.createHash("sha256").update(key).digest("hex");
    return parseInt(hex.slice(0, 8), 16);
  }
}
