export class MessageMetadata {
  id: number = 0; // 4 bytes
  ts: number = Date.now(); // 8 bytes (double)
  producerId: number = 0; // 4 bytes
  priority?: number; // 1 byte (0-255)
  ttl?: number; // 4 bytes
  ttd?: number; // 4 bytes
  topic: string = "";
  correlationId?: string;
  routingKey?: string;
  dedupId?: string;
  // Bit flags for optional fields (1 byte)
  get flags(): number {
    return (
      (this.priority !== undefined ? 0x01 : 0) |
      (this.ttl !== undefined ? 0x02 : 0) |
      (this.ttd !== undefined ? 0x04 : 0) |
      (this.correlationId !== undefined ? 0x10 : 0) |
      (this.routingKey !== undefined ? 0x20 : 0) |
      (this.dedupId !== undefined ? 0x40 : 0)
    );
  }
}