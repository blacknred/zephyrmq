import type { JSONSchemaType } from "ajv";
import Ajv from "ajv";
import type { Level } from "level";
import level from "level";
import fs from "node:fs/promises";
import path, { join } from "node:path";
import { clearImmediate, setImmediate } from "node:timers";
import * as snappy from "snappy";
import { HighCapacityBinaryHeapPriorityQueue } from "./binary_heap_priority_queue";
import type { ICodec } from "./codec/binary_codec";
import { ThreadedBinaryCodec } from "./codec/binary_codec.threaded";
import { uniqueIntGenerator } from "./utils";
//
//
//
// MESSAGE
export class MessageMetadata {
  // Fixed-width fields
  id: number = 0; // 4 bytes
  ts: number = Date.now(); // 8 bytes (double)
  producerId: number = 0; // 4 bytes
  priority?: number; // 1 byte (0-255)
  ttl?: number; // 4 bytes
  ttd?: number; // 4 bytes
  batchId?: number; // 4 bytes
  batchIdx?: number; // 2 bytes
  batchSize?: number; // 2 bytes
  attempts: number = 1; // 1 byte
  consumedAt?: number; // 8 bytes
  size: number = 0;
  needAcks: number = 0;

  // Variable-width fields
  topic: string = "";
  correlationId?: string;
  routingKey?: string;

  // Bit flags for optional fields (1 byte)
  get flags(): number {
    return (
      (this.priority !== undefined ? 0x01 : 0) |
      (this.ttl !== undefined ? 0x02 : 0) |
      (this.ttd !== undefined ? 0x04 : 0) |
      (this.batchId !== undefined ? 0x08 : 0) |
      (this.correlationId !== undefined ? 0x10 : 0) |
      (this.routingKey !== undefined ? 0x20 : 0)
    );
  }
}
interface IMessageValidator<Data> {
  validate(data: { data: Data; meta: MessageMetadata }): void;
}
class SchemaValidator<Data> implements IMessageValidator<Data> {
  constructor(
    private schemaRegistry: ISchemaRegistry,
    private schema: string
  ) {}

  validate({ data }): void {
    const validator = this.schemaRegistry.getValidator(this.schema);
    if (!!validator && !validator(data)) {
      // @ts-ignore
      throw new Error(validator.errors);
    }
  }
}
class SizeValidator implements IMessageValidator<any> {
  constructor(private maxSize: number) {}

  validate({ meta }) {
    if (meta.size > this.maxSize) throw new Error("Message too large");
  }
}
class CapacityValidator implements IMessageValidator<any> {
  constructor(
    private topicMaxCapacity: number,
    private getTopicCapacity: () => number
  ) {}
  validate({ meta }) {
    if (this.getTopicCapacity() + meta.size > this.topicMaxCapacity) {
      throw new Error(`Exceeds topic max size ${this.topicMaxCapacity}`);
    }
  }
}
interface MetadataInput
  extends Pick<MessageMetadata, "priority" | "correlationId" | "ttd" | "ttl"> {}
interface IMessageCreationResult {
  meta: MessageMetadata;
  message?: Buffer;
  error?: any;
}
interface IMessageFactory<Data> {
  create(
    batch: Data[],
    metadataInput: MetadataInput & {
      topic: string;
      producerId: number;
    }
  ): Promise<IMessageCreationResult[]>;
}
class MessageFactory<Data> implements IMessageFactory<Data> {
  constructor(
    private codec: ICodec,
    private validators: IMessageValidator<Data>[]
  ) {}

  async create(
    batch: Data[],
    metadataInput: MetadataInput & { topic: string; producerId: number }
  ) {
    const batchId = Date.now();
    return Promise.all(
      batch.map(async (data, index) => {
        const meta = new MessageMetadata();
        Object.assign(meta, metadataInput);
        meta.id = uniqueIntGenerator();
        meta.ts = Date.now();
        meta.attempts = 1;
        meta.needAcks = 0;

        if (batch.length > 1) {
          meta.batchId = batchId;
          meta.batchIdx = index;
          meta.batchSize = batch.length;
        }

        try {
          const message = await this.codec.encode(data);
          meta.size = message.byteLength;
          this.validators.forEach((v) => v.validate({ data, meta }));
          return { meta, message };
        } catch (error) {
          return { meta, error };
        }
      })
    );
  }
}
//
//
//
// TODO: for n-processes use proper-lockfile
class Mutex {
  private _queue: (() => void)[] = [];
  private _locked = false;

  acquire(): Promise<void> {
    return new Promise((resolve) => {
      const run = () => {
        this._locked = true;
        resolve();
      };

      if (!this._locked) {
        run();
      } else {
        this._queue.push(run);
      }
    });
  }

  release(): void {
    if (this._queue.length > 0) {
      const next = this._queue.shift();
      next?.();
    } else {
      this._locked = false;
    }
  }
}
// WAL: message durability, write as fast as possible: setImmediate flush + no snappy
class WriteAheadLog {
  private fileHandle: fs.FileHandle;
  private flushPromise?: Promise<void>;
  private isFlushing = false;
  private batch: Buffer[] = [];
  private batchSize = 0;

  constructor(
    private filePath: string,
    private maxBatchSizeBytes = 100 * 1024 * 1024
  ) {}

  // init

  async initialize() {
    await fs.mkdir(path.dirname(this.filePath), { recursive: true });
    this.fileHandle = await fs.open(this.filePath, "a+");
  }

  // api

  async append(data: Buffer): Promise<number> {
    const { size: offset } = await this.fileHandle.stat();
    this.batch.push(data);
    this.batchSize += data.length;

    if (this.batchSize > this.maxBatchSizeBytes) {
      this.scheduleFlush();
    }

    return offset;
  }

  async read(offset: number, length: number): Promise<Buffer> {
    const buffer = Buffer.alloc(length);
    await this.fileHandle.read(buffer, 0, length, offset);
    return buffer;
  }

  async close() {
    if (this.batch.length > 0) {
      await this.scheduleFlush();
    }
    await this.fileHandle.close();
  }

  // misc

  private scheduleFlush(): Promise<void> {
    if (this.isFlushing) return this.flushPromise!;
    this.isFlushing = true;

    this.flushPromise = new Promise<void>(async (resolve, reject) => {
      // batched writes
      setImmediate(async () => {
        try {
          const toWrite = Buffer.concat(this.batch);
          await this.fileHandle.write(toWrite);
          this.batch = [];
          this.batchSize = 0;
          resolve();
        } catch (err) {
          reject(err);
        } finally {
          this.isFlushing = false;
          this.flushPromise = undefined;
          if (this.batch.length > 0) {
            this.scheduleFlush();
          }
        }
      });
    });

    return this.flushPromise;
  }

  [Symbol.asyncDispose]() {
    return this.close();
  }
}
// SegmentLog: message persistence, snappy, +fast_read(scan_within_segments), +easy_retention(delete_whole_segment), +compactable(create_new_segments_with_only_actual_data)
type SegmentInfo = {
  id: number;
  filePath: string;
  baseOffset: number;
  size: number;
};
type SegmentRecord = {
  segmentId: number;
  offset: number;
  length: number;
};
class SegmentLog {
  private segments = new Map<number, SegmentInfo>();
  private currentSegment?: SegmentInfo;
  private mutex = new Mutex();

  constructor(
    private baseDir: string,
    private maxSegmentSizeBytes = 100 * 1024 * 1024
  ) {}

  // init

  async initialize() {
    await fs.mkdir(this.baseDir, { recursive: true });
    await this.loadExistingSegments();
    await this.ensureCurrentSegment();
  }

  private async loadExistingSegments() {
    const files = await fs.readdir(this.baseDir);
    for (const file of files.sort()) {
      if (!file.endsWith(".segment")) continue;
      const id = parseInt(file.split(".")[0], 10);
      const { size } = await fs.stat(path.join(this.baseDir, file));
      this.segments.set(id, {
        filePath: path.join(this.baseDir, file),
        baseOffset: 0, // Would need recovery logic
        size,
        id,
      });
    }
  }

  private async ensureCurrentSegment() {
    if (
      this.currentSegment &&
      this.currentSegment.size < this.maxSegmentSizeBytes
    ) {
      return;
    }

    let newId = 0;
    if (this.segments.size > 0) {
      newId = Math.max(...this.segments.keys()) + 1;
    }

    const filePath = path.join(this.baseDir, `${newId}.segment`);
    this.currentSegment = {
      id: newId,
      filePath,
      baseOffset: 0,
      size: 0,
    };

    this.segments.set(newId, this.currentSegment);
  }

  // api

  async append(data: Buffer): Promise<SegmentRecord> {
    await this.ensureCurrentSegment();
    await this.mutex.acquire();
    const segment = this.currentSegment!;

    try {
      const compressed = await snappy.compress(data);
      const lengthBuffer = Buffer.alloc(4);
      lengthBuffer.writeUInt32BE(compressed.length, 0);

      const fileHandle = await fs.open(segment.filePath, "a");

      try {
        await fileHandle.write(lengthBuffer);
        await fileHandle.write(compressed);

        return {
          segmentId: this.currentSegment!.id,
          offset: this.currentSegment!.size,
          length: compressed.length,
        } as SegmentRecord;
      } finally {
        await fileHandle.close();
      }
    } finally {
      await this.mutex.release();
    }
  }

  async read(pointer: SegmentRecord): Promise<Buffer> {
    const segment = this.segments.get(pointer.segmentId);
    if (!segment) throw new Error(`Segment ${pointer.segmentId} not found`);

    const fileHandle = await fs.open(segment.filePath, "r");

    try {
      const compressed = Buffer.alloc(pointer.length);
      await fileHandle.read(compressed, 0, pointer.length, pointer.offset);
      return snappy.uncompress(compressed);
    } finally {
      await fileHandle.close();
    }
  }

  // cleanup

  async cleanupOldSegments(retentionMs: number) {
    const cutoff = Date.now() - retentionMs;
    for (const segment of this.segments.values()) {
      if (segment.id === this.currentSegment?.id) continue;

      const stats = await fs.stat(segment.filePath);
      if (stats.mtimeMs < cutoff) {
        await fs.unlink(segment.filePath);
        this.segments.delete(segment.id);
      }
    }
  }

  async compact() {
    const activeMessageIds = new Set<number>();

    // 1. Scan all active messages
    const metaStream = this.metadataDb.createReadStream({
      gt: "meta!",
      lt: "meta!~",
    });

    for await (const { key, value } of metaStream) {
      const id = parseInt(key.slice(5), 10);
      activeMessageIds.add(id);
    }

    // 2. Create new compacted segments
    const newSegment = await this.createNewSegment();
    let newOffset = 0;

    // 3. Copy only active messages
    for (const segment of this.segments) {
      const fileHandle = await fs.open(segment.filePath, "r");
      let offset = 0;

      while (offset < segment.size) {
        const lengthBuffer = Buffer.alloc(4);
        await fileHandle.read(lengthBuffer, 0, 4, offset);
        const length = lengthBuffer.readUInt32BE(0);

        const messageBuffer = Buffer.alloc(length);
        await fileHandle.read(messageBuffer, 0, length, offset + 4);
        const meta = await this.codec.decodeMetadata(messageBuffer);

        if (activeMessageIds.has(meta.id)) {
          // Write to new segment
          await newSegment.fileHandle.write(lengthBuffer);
          await newSegment.fileHandle.write(messageBuffer);

          // Update pointer
          const newPointer: SegmentRecord = {
            segmentId: newSegment.id,
            offset: newOffset,
            length,
          };
          await this.pointersDb.put(
            `ptr!${meta.id}`,
            JSON.stringify(newPointer)
          );

          newOffset += 4 + length;
        }

        offset += 4 + length;
      }

      await fileHandle.close();
    }

    // 4. Replace old segments
    this.segments = [newSegment];
    this.currentSegment = newSegment;
  }
}
//
//
//
// Storage
interface IFlushable {
  flush: () => Promise<void>;
}
class FlushManager {
  private tasks: Array<IFlushable> = [];
  private timer?: NodeJS.Timeout;

  constructor(private intervalMs: number = 1000) {}

  register(task: IFlushable) {
    this.tasks.push(task);
  }

  start() {
    if (this.intervalMs === Infinity) return;
    this.timer = setInterval(this.flush, this.intervalMs);
    process.on("beforeExit", this.flush);
  }

  stop() {
    clearInterval(this.timer);
    this.timer = undefined;
  }

  private flush = async () => {
    await Promise.all(this.tasks.map((t) => t.flush()));
  };
}
interface IMessageStorage<Data> {
  writeAll(message: Buffer, meta: MessageMetadata): Promise<number>;
  readAll(
    id: number
  ): Promise<
    [
      Awaited<Data> | undefined,
      Pick<MessageMetadata, keyof MessageMetadata> | undefined,
    ]
  >;
  readMessage(id: number): Promise<Data | undefined>;
  readMetadata<K extends keyof MessageMetadata>(
    id: number,
    keys?: K[]
  ): Promise<Pick<MessageMetadata, K> | undefined>;
  updateMetadata(id: number, meta: Partial<MessageMetadata>): Promise<void>;
  flush(): Promise<void>;
}
// class LevelDBMessageStorage<Data> implements IMessageStorage<Data> {
//   // separates metadata/messages buffers since we: 1. need read only data/metadata, 2. need update only metadata, 3. need often read metadata (retention timer)
//   private messages = new Map<number, Buffer>();
//   private metadatas = new Map<number, Buffer>();
//   private flushId?: number;

//   constructor(
//     private topic: string,
//     private codec: ICodec,
//     private retentionMs = 86_400_000, // 1day
//     private isPersist = true,
//     private persistThresholdMs = 100,
//     private chunkSize = 50
//   ) {
//     // TODO: timeout retention check meta:
//     // meta.consumedAt ====> delete
//     // meta.ts + meta.ttl < now ====> DLQ
//     // (non-expired & non-consumed) && now - meta.ts >= retentionMs ====> DLQ
//   }

//   async writeAll(message: Buffer, meta: MessageMetadata): Promise<number> {
//     const encodedMeta = await this.codec.encodeMetadata(meta);
//     this.messages.set(meta.id, message);
//     this.metadatas.set(meta.id, encodedMeta);

//     this.scheduleFlush();
//     return this.messages.size;
//   }

//   async readAll(id: number) {
//     return Promise.all([this.readMessage(id), this.readMetadata(id)]);
//   }

//   async readMessage(id: number) {
//     const buffer = this.messages.get(id);
//     if (!buffer) return;
//     return this.codec.decode<Data>(buffer);
//   }

//   async readMetadata<K extends keyof MessageMetadata>(id: number, keys?: K[]) {
//     const buffer = this.metadatas.get(id);
//     if (!buffer) return;
//     return this.codec.decodeMetadata(buffer, keys);
//   }

//   async updateMetadata(id: number, meta: Partial<MessageMetadata>) {
//     const buffer = this.metadatas.get(id);
//     if (!buffer) return;
//     const newBuffer = this.codec.updateMetadata(buffer, meta);
//     this.metadatas.set(id, newBuffer);
//   }

//   private scheduleFlush() {
//     if (!this.isPersist) return;
//     this.flushId ??= setImmediate(this.flush, this.persistThresholdMs);
//   }

//   flush = async () => {
//     this.flushId = undefined;
//     let count = 0;
//     const idIterator = this.metadatas.keys();

//     for (let id of idIterator) {
//       if (this.chunkSize && count >= this.chunkSize) break;
//       // leveldb put
//       this.messages.delete(id);
//       this.metadatas.delete(id);
//       count++;
//     }

//     if (this.metadatas.size > 0) {
//       this.scheduleFlush();
//     }
//   };
// }
class MessageStorage<Data> implements IMessageStorage<Data> {
  private wal: WriteAheadLog;
  private log: SegmentLog;
  private metadataDb: Level;
  private pointersDb: Level;
  private flushTimer?: NodeJS.Timeout;
  private retentionTimer?: NodeJS.Timeout;

  constructor(
    private topic: string,
    private codec: ICodec,
    private baseDir: string,
    private retentionMs = 86_400_000,
    private flushIntervalMs = 100
  ) {}

  // init

  async initialize() {
    const topicDir = path.join(this.baseDir, this.topic);

    // Initialize components
    // Pointers are not part of metadata since:
    // 1. Metadata changes frequently but Message bodies are immutable after writing.
    // 2. Level performance better with smaller values
    this.pointersDb = level(path.join(topicDir, "pointers"));
    this.metadataDb = level(path.join(topicDir, "metadata"));
    this.wal = new WriteAheadLog(path.join(topicDir, "wal.log"));
    this.log = new SegmentLog(path.join(topicDir, "segments"));

    await Promise.all([this.wal.initialize(), this.log.initialize()]);

    this.startFlushInterval();
    this.startRetentionMonitor();
  }

  private startFlushInterval() {
    if (this.flushIntervalMs === Infinity) return;

    this.flushTimer = setInterval(async () => {
      await this.flush();
    }, this.flushIntervalMs);
  }

  private startRetentionMonitor() {
    if (this.retentionMs === Infinity) return;

    this.retentionTimer = setInterval(
      async () => {
        await this.cleanupExpired();
      },
      Math.min(this.retentionMs, 3600000)
    ); // Check at most hourly
  }

  async flush() {
    await this.wal.scheduleFlush();
  }

  async cleanupExpired() {
    const now = Date.now();
    const cutoff = now - this.retentionMs;

    // Find expired messages by timestamp index
    const stream = this.metadataDb.createReadStream({
      gt: `ts!0`,
      lt: `ts!${cutoff}`,
    });

    for await (const { key, value: id } of stream) {
      await this.deleteMessage(parseInt(id, 10));
    }

    // Cleanup old segments
    await this.log.cleanupOldSegments(this.retentionMs);
  }

  // api

  async writeAll(message: Buffer, meta: MessageMetadata): Promise<number> {
    // 1. Write to WAL first for durability
    const walOffset = await this.wal.append(message);
    // 2. Write to log for long-term storage
    const pointer = await this.log.append(message);
    // 3. Store metadata, pointers and ts (for retention check) in Level
    const metaBuffer = await this.codec.encodeMetadata(meta);
    // TODO: this.codec.encodePointer(pointer)
    const pointerBuffer = await this.codec.encode(pointer);

    await this.metadataDb.batch([
      { type: "put", key: `meta!${meta.id}`, value: metaBuffer },
      { type: "put", key: `ptr!${meta.id}`, value: pointerBuffer },
      { type: "put", key: `ts!${meta.ts}`, value: meta.id.toString() },
    ]);

    return meta.id;
  }

  async readAll(id: number) {
    const result: [
      Awaited<Data> | undefined,
      Pick<MessageMetadata, keyof MessageMetadata> | undefined,
    ] = [undefined, undefined];

    // 1. Get metadata
    const metaBuffer = await this.metadataDb.get(`meta!${id}`);
    if (metaBuffer) {
      const meta = await this.codec.decodeMetadata(metaBuffer);
      result[1] = meta;
    }

    // 2. Get pointer from Level
    const pointerBuffer = await this.pointersDb.get(`ptr!${id}`);
    if (pointerBuffer) {
      const pointer = await this.codec.decode<SegmentRecord>(pointerBuffer);

      // 3. Read message from segments
      const messageBuffer = await this.log.read(pointer);
      if (messageBuffer) {
        const message = await this.codec.decode<Data>(messageBuffer);
        result[0] = message;
      }
    }

    return result;
  }

  async deleteMessage(id: number) {
    await this.metadataDb.batch([
      { type: "del", key: `meta!${id}` },
      { type: "del", key: `ptr!${id}` },
      // Note: We don't delete the timestamp index here for simplicity
      // In production you'd need to track the ts->id mapping separately
    ]);
  }

  async readMessage(id: number): Promise<Data | undefined> {
    const pointerBuffer = await this.pointersDb.get(`ptr!${id}`);
    if (!pointerBuffer) return;
    const pointer = await this.codec.decode<SegmentRecord>(pointerBuffer);

    const messageBuffer = await this.log.read(pointer);
    if (messageBuffer) return;
    return this.codec.decode<Data>(messageBuffer);
  }

  async readMetadata<K extends keyof MessageMetadata>(
    id: number,
    keys?: K[]
  ): Promise<Pick<MessageMetadata, K> | undefined> {
    const metaBuffer = await this.metadataDb.get(`meta!${id}`);
    if (metaBuffer) return;
    const meta = await this.codec.decodeMetadata(metaBuffer);
    if (!keys) return meta;
    const result = {} as Pick<MessageMetadata, K>;

    for (const key of keys) {
      result[key] = meta[key];
    }

    return result;
  }

  async updateMetadata(
    id: number,
    meta: Partial<MessageMetadata>
  ): Promise<void> {
    const metaBuffer = await this.metadataDb.get(`meta!${id}`);
    if (metaBuffer) return;
    const newMetaBuffer = await this.codec.updateMetadata(metaBuffer, meta);
    await this.metadataDb.batch([
      { type: "put", key: `meta!${id}`, value: newMetaBuffer },
    ]);
  }

  async close() {
    clearInterval(this.flushTimer);
    clearInterval(this.retentionTimer);
    await Promise.all([
      this.wal.close(),
      this.metadataDb.close(),
      this.pointersDb.close(),
    ]);
  }

  // Recovery on Startup

  async recover() {
    // 1. Rebuild from WAL first (most recent messages)
    await this.recoverFromWal();

    // 2. Rebuild any missing pointers from segments
    await this.rebuildSegmentIndex();

    // 3. Cleanup any orphaned data
    await this.cleanupOrphans();
  }

  private async recoverFromWal() {
    const walSize = (await this.wal.fileHandle.stat()).size;
    let position = 0;

    while (position < walSize) {
      const lengthBuffer = await this.wal.read(position, 4);
      const length = lengthBuffer.readUInt32BE(0);
      const messageBuffer = await this.wal.read(position + 4, length);

      // Reprocess the message
      const meta = await this.codec.decodeMetadata(messageBuffer);
      const pointer = await this.log.append(messageBuffer);

      await this.metadataDb.batch([
        { type: "put", key: `meta!${meta.id}`, value: JSON.stringify(meta) },
        { type: "put", key: `ptr!${meta.id}`, value: JSON.stringify(pointer) },
      ]);

      position += 4 + length;
    }
  }

  private async rebuildSegmentIndex() {
    for (const segment of this.log.segments) {
      let offset = 0;
      const fileHandle = await fs.open(segment.filePath, "r");

      try {
        while (offset < segment.size) {
          const lengthBuffer = Buffer.alloc(4);
          await fileHandle.read(lengthBuffer, 0, 4, offset);
          const length = lengthBuffer.readUInt32BE(0);

          // Check if we have metadata for this message
          const messageBuffer = Buffer.alloc(length);
          await fileHandle.read(messageBuffer, 0, length, offset + 4);
          const meta = await this.codec.decodeMetadata(messageBuffer);

          if (!(await this.metadataDb.get(`meta!${meta.id}`))) {
            // Orphaned message - could delete or keep
          }

          offset += 4 + length;
        }
      } finally {
        await fileHandle.close();
      }
    }
  }
}
interface ISerializable<T = unknown, R = unknown> {
  serialize(data: T): R;
  deserialize(data: R): T;
}
class PersistedMap<K, V> extends Map<K, V> {
  private dirtyKeys = new Set<K>();
  private mutex = new Mutex();
  private isCleared = false;

  constructor(
    private db: Level,
    private prefix: string,
    private codec: ICodec,
    private serializer?: ISerializable<V>,
    private maxSize = Infinity
  ) {
    super();
    this.restore();
  }

  set(key: K, value: V) {
    if (super.get(key) !== value) {
      this.dirtyKeys.add(key);
    }

    this.evictIfFull();
    return super.set(key, value);
  }

  delete(key: K) {
    this.dirtyKeys.add(key);
    return super.delete(key);
  }

  clear() {
    this.isCleared = true;
    return super.clear();
  }

  // misc
  private evictIfFull(): void {
    // LRU-style eviction (simplified)
    if (this.size < this.maxSize) return;
    const firstKey = this.keys().next().value;
    this.delete(firstKey);
  }

  async restore() {
    try {
      for await (const [key, value] of this.db.iterator({
        gt: `${this.prefix}!`,
        lt: `${this.prefix}~`,
      })) {
        const restoredValue = await this.codec.decode<V>(value);
        const restoredKey = key.slice(this.prefix.length + 1) as K;
        const data = this.serializer?.deserialize(restoredKey) ?? restoredValue;
        super.set(restoredKey, data);
      }
    } catch (e) {
      console.error(`Failed to restore PersistedMap: ${e}`);
    }
  }

  async flush() {
    if (this.dirtyKeys.size === 0) return;

    await this.mutex.acquire();
    try {
      if (this.isCleared) {
        await this.db.clear({
          gt: `${this.prefix}!`,
          lt: `${this.prefix}~`,
        });

        this.dirtyKeys.clear();
        this.isCleared = false;
        return;
      }

      const batch = this.db.batch();
      const encodePromises: Promise<void>[] = [];

      for (const key of this.dirtyKeys) {
        const value = super.get(key);

        if (value !== undefined) {
          encodePromises.push(
            this.codec.encode(value).then((encoded) => {
              const data = this.serializer?.serialize(encoded) ?? encoded;
              batch.put(`${this.prefix}!${key}`, data);
            })
          );
        } else {
          batch.del(`${this.prefix}!${key}`);
        }
      }

      await Promise.all(encodePromises);
      await batch.write();
      this.dirtyKeys.clear();
    } catch (error) {
      console.error(`Failed to flush PersistedMap: ${error}`);
    } finally {
      this.mutex.release();
    }
  }
}
class PersisteMapFactory {
  constructor(
    private topic: string,
    private db: Level,
    private codec: ICodec,
    private flushManager?: FlushManager
  ) {}

  create<K, V>(
    _prefix: string,
    serializer?: ISerializable<V>,
    maxSize = Infinity
  ) {
    const prefix = `${this.topic}:${_prefix}`;
    const map = new PersistedMap<K, V>(
      this.db,
      prefix,
      this.codec,
      serializer,
      maxSize
    );
    this.flushManager?.register(map);
    return map;
  }
}
class PersistedQueue<T> extends HighCapacityBinaryHeapPriorityQueue {
  private pendingOps: Array<["enqueue" | "dequeue", T?]> = [];

  constructor(
    private db: Level,
    private prefix: string,
    private codec: ICodec,
    private maxSize = Infinity,
    flushManager?: FlushManager
    // private QueueFactory: new () => IPriorityQueue<T>,
  ) {
    super();
    this.restore();
    flushManager?.register(this);
  }

  enqueue(item: T, priority?: number) {
    this.queue.enqueue(item, priority);
    this.pendingOps.push(["enqueue", item]);
  }

  dequeue() {}

  // misc

  async restore() {
    const serialized = await this.db.get(`${this.prefix}!state`);
    if (serialized) {
      this.queue = deserialize(serialized);
    }
  }

  async flush() {
    if (this.pendingOps.length === 0) return;

    await this.db.put(`${this.prefix}!state`, serialize(this.queue));
    this.pendingOps = [];
  }
}
//
//
//
//
//
//

class FlushManagerr {
  private flushQueue: (() => Promise<void>)[] = [];
  private pendingFlush = false;
  private flushTimer?: number;

  constructor(
    private readonly flushIntervalMs = 500,
    private readonly maxPendingWrites = 1000
  ) {}

  enqueue(task: () => Promise<void>): void {
    this.flushQueue.push(task);
    if (this.flushQueue.length >= this.maxPendingWrites) {
      this.scheduleFlush();
    }
  }

  scheduleFlush(delay = this.flushIntervalMs): void {
    if (this.pendingFlush || !this.flushIntervalMs) return;
    this.flushTimer = setTimeout(() => {
      this.flushNow();
    }, delay);
  }

  async flushNow(): Promise<void> {
    if (this.pendingFlush) return;
    this.pendingFlush = true;

    const tasks = [...this.flushQueue];
    this.flushQueue = [];

    await Promise.all(tasks.map((task) => task()));
    this.pendingFlush = false;
  }

  stop(): void {
    clearTimeout(this.flushTimer);
    this.flushNow();
  }
}
class LogSegment {
  private fd: number;
  private offset = 0;

  constructor(private filePath: string) {
    this.fd = fs.openSync(filePath, "a");
  }

  static create(basePath: string, segmentId: number): LogSegment {
    const filename = path.join(basePath, `segment-${segmentId}.log`);
    return new LogSegment(filename);
  }

  append(message: Buffer): number {
    const writeOffset = this.offset;
    fs.writeSync(this.fd, message);
    this.offset += message.length;
    return writeOffset;
  }

  read(offset: number, length: number): Buffer {
    const buffer = Buffer.alloc(length);
    fs.readSync(this.fd, buffer, 0, length, offset);
    return buffer;
  }

  close(): void {
    fs.closeSync(this.fd);
  }
}
interface MessageRecord {
  id: number;
  offset: number;
  size: number;
}
class MessageStore {
  private currentSegment!: LogSegment;
  private indexDB: Level<string, MessageRecord>;
  private segmentSizeLimit = 1024 * 1024 * 100; // 100 MB
  private currentSegmentBytes = 0;

  constructor(private baseDir: string) {
    this.indexDB = new Level<MessageRecord>(join(baseDir, "index"));
    this.rollNewSegment();
  }

  private rollNewSegment() {
    if (this.currentSegment) this.currentSegment.close();
    const segmentId = Date.now();
    this.currentSegment = LogSegment.create(this.baseDir, segmentId);
    this.currentSegmentBytes = 0;
  }

  async write(id: number, message: Buffer): Promise<void> {
    const offset = this.currentSegment.append(message);
    const record = {
      id,
      offset,
      size: message.length,
    };
    await this.indexDB.put(`msg:${id}`, record);
    this.currentSegmentBytes += message.length;
    if (this.currentSegmentBytes > this.segmentSizeLimit) {
      this.rollNewSegment();
    }
  }

  async read(id: number): Promise<Buffer | null> {
    const record = await this.indexDB.get(`msg:${id}`);
    if (!record) return null;
    return this.currentSegment.read(record.offset, record.size);
  }

  flush(): Promise<void> {
    return this.indexDB.flush();
  }
}
//
//
//
// SRC/PUBLISHING_SERVICE.TS
interface IMessageProcessor {
  process(meta: MessageMetadata): boolean;
}
class ExpirationProcessor<Data> implements IMessageProcessor {
  constructor(private dlq: IDLQManager<Data>) {}
  process(meta: MessageMetadata): boolean {
    if (!meta.ttl) return false;
    const isAlreadyOld = meta.ts + meta.ttl <= Date.now();
    const isWillBeOldWnenDelayIsOver = !!meta.ttd && meta.ttd > meta.ttl;
    const isExpired = isAlreadyOld || isWillBeOldWnenDelayIsOver;
    if (isExpired) this.dlq.enqueue(meta, "expired");
    return isExpired;
  }
}
class AttemptsProcessor<Data> implements IMessageProcessor {
  constructor(
    private dlq: IDLQManager<Data>,
    private maxAttempts: number
  ) {}
  process(meta: MessageMetadata): boolean {
    const shouldDeadLetter = meta.attempts > this.maxAttempts;
    if (shouldDeadLetter) {
      this.dlq.enqueue(meta, "max_attempts");
    }
    return shouldDeadLetter;
  }
}
class DelayProcessor implements IMessageProcessor {
  constructor(private delayedMessageManager: IDelayedMessageManager) {}
  process(meta: MessageMetadata): boolean {
    if (!meta.ttd) return false;
    const shouldDelay = meta.ts + meta.ttd! > Date.now();
    if (shouldDelay) this.delayedMessageManager.enqueue(meta);
    return shouldDelay;
  }
}
interface IMessagePipeline {
  addProcessor(processor: IMessageProcessor): void;
  process(meta: MessageMetadata): boolean;
}
class MessagePipeline implements IMessagePipeline {
  private processors: IMessageProcessor[] = [];
  addProcessor(processor: IMessageProcessor): void {
    this.processors.push(processor);
  }
  process(meta: MessageMetadata): boolean {
    for (const processor of this.processors) {
      if (processor.process(meta)) return true;
    }
    return false;
  }
}
interface IPipelineFactory<Data> {
  create(
    dlqManager: DLQManager<Data>,
    delayedMessageManager: DelayedMessageManager<Data>,
    maxAttempts?: number
  ): IMessagePipeline;
}
class PipelineFactory<Data> implements IPipelineFactory<Data> {
  create(
    dlqManager: IDLQManager<Data>,
    delayedMessageManager: IDelayedMessageManager,
    maxAttempts?: number
  ) {
    const pipeline = new MessagePipeline();
    pipeline.addProcessor(new ExpirationProcessor(dlqManager));
    pipeline.addProcessor(new DelayProcessor(delayedMessageManager));
    if (maxAttempts !== undefined) {
      pipeline.addProcessor(new AttemptsProcessor(dlqManager, maxAttempts));
    }

    return pipeline;
  }
}
interface IDelayScheduler<Data> {
  setReadyCallback(onReadyHandler: (data: Data) => Promise<void>): void;
  schedule(data: Data, readyTs: number): void;
  pendingsCount(): number;
  cleanup(): void;
}
class DelayScheduler<Data> implements IDelayScheduler<Data> {
  private onReadyCallback: (data: Data) => Promise<void>;
  private nextTimeout?: number;
  private isProcessing = false;

  constructor(private queue: IPriorityQueue<[Data, number]>) {}

  setReadyCallback(onReadyHandler: (data: Data) => Promise<void>) {
    this.onReadyCallback = onReadyHandler;
  }

  schedule(data: Data, readyTs: number) {
    this.queue.enqueue([data, readyTs], readyTs);
    this.setNextTimeout();
  }

  private setNextTimeout(): void {
    if (this.isProcessing || this.queue.isEmpty()) return;

    const record = this.queue.peek();
    if (!record) return;

    const delay = Math.max(0, record[1] - Date.now());
    clearTimeout(this.nextTimeout);
    this.nextTimeout = setTimeout(this.onTimeoutHandler, delay);
  }

  private onTimeoutHandler = () => {
    if (this.isProcessing) return;
    this.isProcessing = true;
    const now = Date.now();

    try {
      while (!this.queue.isEmpty()) {
        const [data, readyTs] = this.queue.peek()!;
        if (readyTs > now) break; // second peak is not ready yet
        this.queue.dequeue();
        this.onReadyCallback(data);
      }
    } finally {
      this.isProcessing = false;
      this.setNextTimeout();
    }
  };

  pendingsCount() {
    return this.queue.size();
  }

  cleanup() {
    clearTimeout(this.nextTimeout);
  }
}
interface IDelayedMessageManager {
  enqueue(meta: MessageMetadata): void;
  getMetadata(): {
    count: number;
  };
}
class DelayedMessageManager<Data> implements IDelayedMessageManager {
  constructor(
    private messageScheduler: IDelayScheduler<number>,
    private messageStorage: IMessageStorage<Data>,
    private messageRouter: IMessageRouter,
    private deliveryCounter: IDeliveryCounter,
    private logger?: ILogCollector
  ) {
    messageScheduler.setReadyCallback(this.dequeue);
  }

  enqueue(meta: MessageMetadata) {
    if (!meta.ttd) return;
    this.messageScheduler.schedule(meta.id, meta.ts + meta.ttd);
  }

  private dequeue = async (messageId: number) => {
    const meta = await this.messageStorage.readMetadata(messageId);
    if (!meta) return;

    const deliveryCount = await this.messageRouter.route(meta);
    this.deliveryCounter.setAwaitedDeliveries(meta.id, deliveryCount);

    this.logger?.log(`Message is routed to ${meta.topic}.`, meta);
  };

  getMetadata() {
    return {
      count: this.messageScheduler.pendingsCount(),
    };
  }
}
interface IConsumerGroup {
  addMember(id: string, routingKeys?: string[]): void;
  removeMember(id: string): void;
  hasMembers(): boolean;
  getMembers(
    messageId: number,
    correlationId?: string
  ): Iterable<string> | undefined;
  getName(): string | undefined;
  getMemberRoutingKeys(id: string): Set<string> | undefined;
  getMetadata(): {
    name: string;
    count: number;
  };
}
class ConsumerGroup implements IConsumerGroup {
  private members: PersistedMap<string, Array<string>>;
  constructor(
    private name: string | undefined,
    mapFactory: PersisteMapFactory,
    private hashRing: IHashRing
  ) {
    this.members = mapFactory.create<string, Array<string>>(
      `groups:${name}:members`
    );
  }

  addMember(id: string, routingKeys?: string[]) {
    const keysSet = new Set(routingKeys);

    // for the group with defined groupId we need to enforce homogeneous routingKeys within the members
    if (this.name && this.members.size > 0) {
      const expectedKeys = this.members.values().next().value as
        | Set<string>
        | undefined;
      const isValid =
        expectedKeys?.size === keysSet.size &&
        [...keysSet].every((k) => expectedKeys.has(k));

      if (!isValid) {
        throw new Error(
          `Member ${id} has incompatible routingKeys for group ${this.name}`
        );
      }
    }

    this.hashRing.addNode(id);
    this.members.set(id, routingKeys);
  }

  removeMember(id: string) {
    this.hashRing.removeNode(id);
    this.members.delete(id);
  }

  hasMembers(): boolean {
    return this.hashRing.getNodeCount() > 0;
  }

  getMembers(
    messageId: number,
    correlationId?: string
  ): Iterable<string> | undefined {
    if (this.name || correlationId) {
      return this.hashRing.getNode(correlationId || messageId.toString());
    }
    return this.members.keys();
  }

  getName() {
    return this.name;
  }

  getMemberRoutingKeys(id: string) {
    return this.members.get(id);
  }

  getMetadata() {
    return {
      name: this.name || "non-grouped",
      count: this.members.size,
    };
  }
}
interface IMessageRouter {
  addConsumer(id: string, groupId?: string, routingKeys?: string[]): void;
  removeConsumer(id: string): void;
  route(meta: MessageMetadata): Promise<number>;
  routeBatch(metas: MessageMetadata[]): Promise<number[]>;
  getMetadata(): {
    consumerGroups: {
      name: string;
      count: number;
    }[];
  };
}
class MessageRouter<Data> implements IMessageRouter {
  private consumerGroups = new Map<string | undefined, IConsumerGroup>();
  // private groups: PersistedMap<string | undefined, IConsumerGroup>;

  constructor(
    private clientManager: IClientManager,
    private queueManager: IQueueManager,
    private subscriptionManager: ISubscriptionManager<Data>,
    private dlqManager: IDLQManager<Data>
  ) {}

  getMetadata() {
    return {
      consumerGroups: Array.from(this.consumerGroups.values()).map((group) =>
        group.getMetadata()
      ),
    };
  }

  addConsumer(id: string, groupId?: string, routingKeys?: string[]) {
    if (!this.consumerGroups.has(groupId)) {
      const hashRing = new InMemoryHashRing(new SHA256HashService());
      this.consumerGroups.set(groupId, new ConsumerGroup(groupId, hashRing));
    }

    this.consumerGroups.get(groupId)!.addMember(id, routingKeys);
  }

  removeConsumer(id: string) {
    for (const [name, group] of this.consumerGroups.entries()) {
      group.removeMember(consumerId);
      if (!group.hasMembers()) {
        this.consumerGroups.delete(name);
      }
    }
  }

  async route(meta: MessageMetadata): Promise<number> {
    const results = await Promise.all(
      Array.from(this.consumerGroups.values()).map((group) =>
        this.groupRoute(group, meta)
      )
    );

    let processedCount = 0;
    let deliveryCount = 0;

    for (const result of results) {
      if (!result) continue;
      processedCount += result.processedCount;
      deliveryCount += result.deliveryCount;
    }

    // all consumers are non-operable or binded to other routingKeys
    if (!processedCount) {
      this.dlqManager.enqueue(meta, "no_consumers");
    }

    return deliveryCount;
  }

  async routeBatch(metas: MessageMetadata[]): Promise<number[]> {
    return Promise.all(metas.map((msg) => this.route(msg)));
  }

  private async groupRoute(group: IConsumerGroup, meta: MessageMetadata) {
    const { routingKey, correlationId, id } = meta;
    const isSingleConsumer = group.getName() || correlationId;
    const candidates = group.getMembers(id, correlationId);
    if (!candidates) return;

    let fallbackCandidateId;
    let processedCount = 0;
    let deliveryCount = 0;
    const now = Date.now();

    for (const candidateId of candidates) {
      // filter candidate
      if (!this.isSuitable(group, candidateId, now, routingKey)) continue;

      // prefer idle consumer for single consumer mode
      if (isSingleConsumer && !this.clientManager.isIdle(candidateId)) {
        fallbackCandidateId ??= candidateId;
        continue;
      }

      // create delivery
      deliveryCount += await this.deliver(candidateId, meta);
      processedCount++;
      fallbackCandidateId = undefined;
      if (isSingleConsumer) break;
    }

    // fallback for single consumer mode
    if (fallbackCandidateId) {
      deliveryCount += await this.deliver(fallbackCandidateId, meta);
      processedCount++;
    }

    return { processedCount, deliveryCount };
  }

  private async deliver(consumerId: number, meta: MessageMetadata) {
    // try to push at first, otherwise enqueue to pull
    if (this.subscriptionManager.hasListener(consumerId)) {
      const needAck = await this.subscriptionManager.pushTo(consumerId, meta);
      return needAck ? 1 : 0;
    }

    this.queueManager.enqueue(consumerId, meta);
    return 1;
  }

  private isSuitable(
    group: IConsumerGroup,
    consumerId: number,
    now: number,
    routingKey?: string
  ) {
    // skip non-operable members (backpressure)
    if (!this.clientManager.isOperable(consumerId, now)) return false;

    // filtered-out
    const expectedKeys = group.getMemberRoutingKeys(consumerId);
    return !expectedKeys?.size || expectedKeys.has(routingKey!);
  }
}
interface IPublishingService {
  publish(
    producerId: string,
    message: Buffer,
    meta: MessageMetadata
  ): Promise<void>;
  getMetadata(): {
    router: {
      consumerGroups: {
        name: string;
        count: number;
      }[];
    };
    delayedMessages: {
      count: number;
    };
  };
}
class PublishingService<Data> implements IPublishingService {
  constructor(
    private readonly messageStorage: IMessageStorage<Data>,
    private readonly pipeline: IMessagePipeline,
    private readonly messageRouter: IMessageRouter,
    private readonly deliveryCounter: IDeliveryCounter,
    private readonly clientManager: IClientManager,
    private readonly delayedManager: IDelayedMessageManager,
    private readonly metrics: IMetricsCollector,
    private readonly logger?: ILogCollector
  ) {}

  async publish(
    producerId: string,
    message: Buffer,
    meta: MessageMetadata
  ): Promise<void> {
    await this.messageStorage.writeAll(message, meta);

    const processingTime = Date.now() - meta.ts;
    this.metrics.recordEnqueue(meta.size, processingTime);
    this.clientManager.recordActivity(producerId, {
      messageCount: 1,
      processingTime,
      status: "idle",
    });

    if (this.pipeline.process(meta)) return;
    const deliveryCount = await this.messageRouter.route(meta);
    this.deliveryCounter.setAwaitedDeliveries(meta.id, deliveryCount);

    this.logger?.log(`Message is routed to ${meta.topic}.`, meta);
  }

  getMetadata() {
    return {
      router: this.messageRouter.getMetadata(),
      delayedMessages: this.delayedManager.getMetadata(),
      // metadata from messageStorage
    };
  }
}
//
//
//
// SRC/CONSUMPTION_SERVICE.TS
interface IPriorityQueue<Data = any> {
  enqueue(data: Data, priority?: number): void;
  dequeue(): Data | undefined;
  peek(): Data | undefined;
  isEmpty(): boolean;
  size(): number;
}
interface IQueueManager {
  addQueue(id: number): void;
  removeQueue(id: number): void;
  enqueue(id: number, meta: MessageMetadata): number | undefined;
  dequeue(id: number): number | undefined;
  getMetadata(): {
    size: number;
  };
}
class QueueManager implements IQueueManager {
  // TODO: persist
  private queues = new Map<number, IPriorityQueue<number>>();
  private totalQueuedMessages: 0;

  constructor(private queueFactory: new () => IPriorityQueue<number>) {}

  addQueue(id: number) {
    this.queues.set(id, new this.queueFactory());
  }

  removeQueue(id: number) {
    this.queues.delete(id);
  }

  enqueue(id: number, meta: MessageMetadata) {
    this.queues.get(id)?.enqueue(meta.id, meta.priority);
    this.totalQueuedMessages++;
    return this.queues.get(id)?.size();
  }

  dequeue(id: number) {
    const messageId = this.queues.get(id)?.dequeue();
    if (messageId) this.totalQueuedMessages--;
    return messageId;
  }

  getMetadata() {
    return {
      size: this.totalQueuedMessages,
    };
  }
}
interface IConsumptionService<Data> {
  consume(consumerId: number, autoAck?: boolean): Promise<Data | undefined>;
  getMetadata(): {
    queuedMessages: {
      size: number;
    };
  };
}
class ConsumptionService<Data> implements IConsumptionService<Data> {
  constructor(
    private readonly queueManager: IQueueManager,
    private readonly messageStorage: IMessageStorage<Data>,
    private readonly pendingAcks: IPendingAcks,
    private readonly deliveryCounter: IDeliveryCounter,
    private readonly clientManager: IClientManager,
    private readonly logger?: ILogCollector
  ) {}

  async consume(
    consumerId: number,
    autoAck = false
  ): Promise<Data | undefined> {
    if (this.pendingAcks.isReachedMaxUnacked(consumerId)) return;

    const messageId = this.queueManager.dequeue(consumerId);
    if (!messageId) return;

    const [message, meta] = await this.messageStorage.readAll(messageId);
    if (!meta || !message) return;

    if (!autoAck) {
      this.pendingAcks.addPending(consumerId, messageId);
    } else {
      this.clientManager.recordActivity(consumerId, {
        messageCount: 1,
        pendingAcks: 0,
        processingTime: 0,
        status: "idle",
      });

      await this.deliveryCounter.decrementAwaitedDeliveries(messageId);
    }

    this.logger?.log(`Message is consumed from ${meta.topic}.`, meta);
    return message;
  }

  getMetadata() {
    return {
      queuedMessages: this.queueManager.getMetadata(),
    };
  }
}
//
//
//
// SRC/ACK_SERVICE.TS
interface IPendingAcks {
  addPending(consumerId: number, messageId: number): void;
  getPendings(consumerId: number): Map<number, number> | undefined;
  getAllPendings(): Map<number, Map<number, number>>;
  removePending(consumerId: number, messageId?: number): void;
  isReachedMaxUnacked(consumerId: number): boolean;
  getMetadata(): {
    count: number;
  };
}
class PendingAcks implements IPendingAcks {
  // TODO: persist
  private pendingAcks: PersistedMap<number, Map<number, number>>;

  constructor(
    mapFactory: PersisteMapFactory,
    private clientManager: IClientManager,
    private maxUnackedPerConsumer = 10
  ) {
    this.pendingAcks = mapFactory.create<number, Map<number, number>>(
      "pending_acks"
    );
  }

  isReachedMaxUnacked(consumerId: number) {
    return this.getPendings(consumerId)?.size === this.maxUnackedPerConsumer;
  }

  getMetadata() {
    let count = 0;
    const consumerAcks = this.pendingAcks.values();
    for (const acks of consumerAcks) {
      count += acks.size;
    }
    return { count };
  }

  addPending(consumerId: number, messageId: number): void {
    if (!this.pendingAcks.has(consumerId)) {
      this.pendingAcks.set(consumerId, new Map());
    }
    this.pendingAcks.get(consumerId)?.set(messageId, Date.now());

    this.clientManager.recordActivity(consumerId, {
      pendingAcks: 1,
      status: "active",
    });
  }

  getPendings(consumerId: number) {
    return this.pendingAcks.get(consumerId);
  }

  getAllPendings() {
    return this.pendingAcks;
  }

  removePending(consumerId: number, messageId?: number): void {
    const pendings = this.pendingAcks.get(consumerId);
    if (!pendings) return;
    const now = Date.now();

    if (messageId) {
      if (pendings.has(messageId)) {
        const consumedAt = pendings.get(messageId)!;
        pendings.delete(messageId);

        this.clientManager.recordActivity(consumerId, {
          pendingAcks: -1,
          messageCount: 1,
          processingTime: now - consumedAt,
          status: pendings.size ? "active" : "idle",
        });
      }
    } else if (pendings.size) {
      const firstConsumeAt = pendings.values().next().value as number;
      this.clientManager.recordActivity(consumerId, {
        pendingAcks: -pendings.size,
        messageCount: pendings.size,
        processingTime: Date.now() - firstConsumeAt,
        status: "idle",
      });

      pendings.clear();
    }

    if (!pendings.size) {
      this.pendingAcks.delete(consumerId);
    }
  }
}
interface IDeliveryCounter {
  setAwaitedDeliveries(messageid: number, deliveries: number): void;
  decrementAwaitedDeliveries(messageId: number): Promise<void>;
}
class DeliveryCounter<Data> implements IDeliveryCounter {
  private deliveries: PersistedMap<number, number>;

  constructor(
    mapFactory: PersisteMapFactory,
    private messageStorage: IMessageStorage<Data>,
    private metrics: IMetricsCollector
  ) {
    this.deliveries = mapFactory.create<number, number>("deliveries");
  }

  setAwaitedDeliveries(messageid: number, deliveries: number) {
    this.deliveries.set(messageid, deliveries);
  }

  async decrementAwaitedDeliveries(messageId: number) {
    let deliveries = this.deliveries.get(messageId);
    if (!deliveries) return;
    this.deliveries.set(messageId, --deliveries);
    if (deliveries > 0) return;

    // if there is no deliveries needed mark message as consumed
    const meta = await this.messageStorage.readMetadata(messageId, ["ts"]);
    if (!meta) return;
    const consumedAt = Date.now();
    await this.messageStorage.updateMetadata(messageId, { consumedAt });
    this.metrics.recordDequeue(consumedAt - meta.ts);
  }
}
interface AckTimeoutHandler {
  (consumerId: number, messageId?: number, requeue?: boolean): Promise<number>;
}
interface IAckMonitor {
  setTimeoutCallback(onTimeoutHandler: AckTimeoutHandler): void;
  stop(): void;
}
class AckMonitor implements IAckMonitor {
  private onTimeoutCallback: AckTimeoutHandler;
  private timer?: number;

  constructor(
    private pendingAcks: IPendingAcks,
    private ackTimeoutMs = 30_000
  ) {
    this.timer = setInterval(
      this.nackTimedOutPendings,
      Math.max(1000, this.ackTimeoutMs / 2)
    );
  }

  setTimeoutCallback(onTimeoutHandler: AckTimeoutHandler) {
    this.onTimeoutCallback = onTimeoutHandler;
  }

  stop() {
    clearInterval(this.timer);
  }

  private nackTimedOutPendings = async () => {
    const now = Date.now();
    const pendings = this.pendingAcks.getAllPendings();
    if (!pendings) return;
    for (const [consumerId, messages] of pendings) {
      for (const [messageId, consumedAt] of messages.entries()) {
        if (now - consumedAt > this.ackTimeoutMs) {
          await this.onTimeoutCallback?.(consumerId, messageId, true);
        }
      }
    }
  };
}
interface IAckService {
  ack(consumerId: number, messageId?: number): Promise<number[]>;
  nack: (
    consumerId: number,
    messageId?: number,
    requeue?: boolean
  ) => Promise<number>;
  getMetadata(): {
    pendingAcks: {
      count: number;
    };
  };
}
class AckService<Data> implements IAckService {
  constructor(
    private readonly pendingAcks: IPendingAcks,
    private readonly deliveryCounter: IDeliveryCounter,
    private readonly ackMonitor: IAckMonitor,
    private readonly messageStorage: IMessageStorage<Data>,
    private readonly pipeline: IMessagePipeline,
    private readonly queueManager: IQueueManager,
    private readonly subscriptionManager: ISubscriptionManager<Data>,
    private readonly logger?: ILogCollector
  ) {
    this.ackMonitor.setTimeoutCallback(this.nack);
  }

  async ack(consumerId: number, messageId?: number): Promise<number[]> {
    const pendingAcks: number[] = [];

    if (messageId) {
      pendingAcks.push(messageId);
      this.pendingAcks.removePending(consumerId, messageId);
    } else {
      const pendingMap = this.pendingAcks.getPendings(consumerId);
      if (pendingMap) pendingAcks.push(...pendingMap.keys());
      this.pendingAcks.removePending(consumerId);
    }

    for (const messageId of pendingAcks) {
      await this.deliveryCounter.decrementAwaitedDeliveries(messageId);
    }

    // try to drainQueue if push mode is available
    if (this.subscriptionManager.hasListener(consumerId)) {
      this.subscriptionManager.drainQueue(consumerId);
    }

    return pendingAcks;
  }

  nack = async (consumerId: number, messageId?: number, requeue = true) => {
    const messages = await this.ack(consumerId, messageId);

    for (const messageId of messages) {
      const meta = await this.messageStorage.readMetadata(messageId);
      if (!meta) continue;

      await this.messageStorage.updateMetadata(messageId, {
        attempts: requeue ? meta.attempts + 1 : Infinity,
        consumedAt: undefined,
      });

      if (this.pipeline.process(meta)) continue;

      this.queueManager.enqueue(consumerId, meta);
      this.logger?.log(
        `Message is nacked to ${requeue ? "queue" : "DLQ"}.`,
        meta,
        "warn"
      );
    }

    return messages.length;
  };

  getMetadata() {
    return {
      pendingAcks: this.pendingAcks.getMetadata(),
    };
  }
}
//
//
//
// SRC/SUBSCRIPTION_SERVICE.TS
type ISubscriptionListener<Data> = (message: Data) => Promise<void>;
interface ISubscriptionManager<Data> {
  addListener(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    autoAck?: boolean
  ): void;
  removeListener(consumerId: number): void;
  hasListener(consumerId: number): boolean;
  pushTo(
    consumerId: number,
    meta: MessageMetadata
  ): Promise<boolean | undefined>;
  drainQueue(consumerId: number): Promise<void>;
  getMetadata(): {
    count: number;
  };
}
class SubscriptionManager<Data> implements ISubscriptionManager<Data> {
  private subscriptions = new Map<
    number,
    [ISubscriptionListener<Data>, boolean]
  >();

  constructor(
    private clientManager: ClientManager,
    private queueManager: QueueManager,
    private messageStorage: IMessageStorage<Data>,
    private pendingAcks: PendingAcks,
    private deliveryCounter: DeliveryCounter<Data>,
    private logger?: ILogCollector
  ) {}

  getMetadata() {
    return {
      count: this.subscriptions.size,
    };
  }

  addListener(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    autoAck = false
  ) {
    this.subscriptions.set(consumerId, [listener, autoAck]);
    this.drainQueue(consumerId);
  }

  removeListener(consumerId: number) {
    this.subscriptions.delete(consumerId);
  }

  hasListener(consumerId: number) {
    return this.subscriptions.has(consumerId);
  }

  async pushTo(consumerId: number, meta: MessageMetadata) {
    const subscription = this.subscriptions.get(consumerId);
    if (!subscription) return;
    const [listener, autoAck] = subscription;

    if (this.pendingAcks.isReachedMaxUnacked(consumerId)) return;

    const message = await this.messageStorage.readMessage(meta.id);
    if (!message) return;

    listener(message);

    setImmediate(() => {
      this.logger?.log(`Message is consumed from ${meta.topic}.`, meta);
    });

    if (!autoAck) {
      this.pendingAcks.addPending(consumerId, meta.id);
      return true;
    }

    this.clientManager.recordActivity(consumerId, {
      messageCount: 1,
      pendingAcks: 0,
      processingTime: 0,
      status: "idle",
    });

    return false;
  }

  async drainQueue(consumerId: number) {
    if (this.pendingAcks.isReachedMaxUnacked(consumerId)) return;

    // process the next message from the queue
    const messageId = this.queueManager.dequeue(consumerId);
    if (!messageId) return;

    const meta = await this.messageStorage.readMetadata(messageId);
    if (!meta) return;

    const needAck = await this.pushTo(consumerId, meta);

    if (!needAck) {
      await this.deliveryCounter.decrementAwaitedDeliveries(messageId);
    }

    // schedule the next drain
    setImmediate(() => this.drainQueue(consumerId));
  }
}
interface ISubscriptionService<Data> {
  subscribe(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    autoAck?: boolean
  ): void;
  unsubscribe(consumerId: number): void;
  getMetadata(): {
    count: number;
  };
}
class SubscriptionService<Data> implements ISubscriptionService<Data> {
  constructor(
    private readonly topicName: string,
    private readonly subscriptionManager: ISubscriptionManager<Data>,
    private readonly logger?: ILogCollector
  ) {}

  subscribe(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    autoAck?: boolean
  ): void {
    this.subscriptionManager.addListener(consumerId, listener, autoAck);

    this.logger?.log(`${consumerId} is subscribed to ${this.topicName}.`, {
      consumerId,
      autoAck,
    });
  }

  unsubscribe(consumerId: number): void {
    this.subscriptionManager.removeListener(consumerId);

    this.logger?.log(`${consumerId} is unsubscribed from ${this.topicName}.`, {
      consumerId,
    });
  }

  getMetadata() {
    return this.subscriptionManager.getMetadata();
  }
}
//
//
//
// SRC/DLQ_SERVICE.TS
type DLQReason =
  | "no_consumers"
  | "expired"
  | "max_attempts"
  | "validation"
  | "processing_error";
interface IDLQEntry<Data> {
  reason: DLQReason;
  message: Data;
  meta: MessageMetadata;
}
interface IDLQManager<Data> {
  enqueue(meta: MessageMetadata, reason: DLQReason): void;
  createReader(): AsyncGenerator<IDLQEntry<Data>, void, unknown>;
  replayMessages(
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ): Promise<number>;
  getMetadata(): {
    size: number;
  };
}
class DLQManager<Data> implements IDLQManager<Data> {
  private messages: PersistedMap<number, DLQReason>;

  constructor(
    private topic: string,
    mapFactory: PersisteMapFactory,
    private messageStorage: IMessageStorage<any>,
    private logger?: ILogCollector
  ) {
    this.messages = mapFactory.create("dlqMessages");
  }

  size() {
    return this.messages.size;
  }

  enqueue(meta: MessageMetadata, reason: DLQReason): void {
    this.messages.set(meta.id, reason);
    this.logger?.log(
      `Message is routed to DLQ. Reason: ${reason}.`,
      meta,
      "warn"
    );
  }

  async *createReader(): AsyncGenerator<IDLQEntry<Data>, void, unknown> {
    for (const [messageId, reason] of this.messages.entries()) {
      const [message, meta] = await this.messageStorage.readAll(messageId);
      if (!meta || !message) continue;
      yield { message, meta, reason };
    }
  }

  async replayMessages(
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ) {
    let count = 0;
    const records = this.createReader();

    for await (const { message, meta } of records) {
      if (!filter || filter(meta)) {
        try {
          await handler(message, meta);
          this.messages.delete(meta.id);
          count++;
        } catch (e) {}
      }
    }

    this.logger?.log(
      `Replayed DLQ messages.`,
      { count, topic: this.topic },
      "warn"
    );

    return count;
  }

  getMetadata() {
    return {
      size: this.messages.size,
    };
  }
}
interface IDLQService<Data> {
  createDlqReader(): AsyncGenerator<IDLQEntry<Data>, void, unknown>;
  replayDlq(
    consumerId: number,
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ): Promise<number>;
  getMetadata(): {
    size: number;
  };
}
class DLQService<Data> implements IDLQService<Data> {
  constructor(
    private readonly dlqManager: IDLQManager<Data>,
    private readonly clientManager: IClientManager,
    private readonly logger?: ILogCollector
  ) {}

  createDlqReader() {
    return this.dlqManager.createReader();
  }

  async replayDlq(
    consumerId: number,
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ) {
    const start = Date.now();
    this.clientManager.recordActivity(consumerId, {
      status: "active",
    });

    const replayedCount = await this.dlqManager.replayMessages(handler, filter);

    this.clientManager.recordActivity(consumerId, {
      messageCount: replayedCount,
      processingTime: Date.now() - start,
      status: "idle",
    });

    return replayedCount;
  }

  getMetadata() {
    return this.dlqManager.getMetadata();
  }
}
//
//
//
// SRC/CLIENT_MANAGEMENT_SERVICE.TS
type ClientType = "producer" | "consumer" | "dlq_consumer";
interface IClientState {
  id: string;
  clientType: ClientType;
  registeredAt: number;
  lastActiveAt: number;
  // Metrics
  status: "active" | "idle" | "lagging";
  messageCount: number;
  processingTime: number;
  avgProcessingTime: number;
  pendingAcks: number;
}
interface IClientManager {
  addClient(type: ClientType, id: string): IClientState;
  removeClient(id: string): number;
  getClients(filter?: (client: IClientState) => boolean): Set<IClientState>;
  getClient(id: string): IClientState | undefined;
  throwIfExists(id: string): void;
  validateClient(id: string, expectedType?: ClientType): void;
  isOperable(id: string, now: number): boolean;
  isIdle(id: string): boolean;
  recordActivity(id: string, activityRecord: Partial<IClientState>): void;
  getMetadata(): {
    count: number;
    producersCount: number;
    consumersCount: number;
    dlqConsumersCount: number;
    operableCount: number;
    idleCount: number;
    avgProcessingTime: number;
  };
}
class ClientManager implements IClientManager {
  private clients = new Map<string, IClientState>();

  constructor(
    private inactivityThresholdMs = 300_000,
    private processingTimeThresholdMs = 50_000,
    private pendingThresholdMs = 100
  ) {}

  addClient(type: ClientType, id: string) {
    const now = Date.now();
    this.clients.set(id, {
      registeredAt: now,
      lastActiveAt: now,
      clientType: type,
      messageCount: 0,
      pendingAcks: 0,
      processingTime: 0,
      avgProcessingTime: 0,
      status: "active",
      id,
    });

    this[`total${type[0].toUpperCase() + type.slice(1)}s`]++;
    return this.clients.get(id)!;
  }

  removeClient(id: string) {
    const client = this.clients.get(id);
    if (client) {
      const type = client.clientType;

      this[`total${type[0].toUpperCase() + type.slice(1)}s`]--;
      this.clients.delete(id);
    }
    return this.clients.size;
  }

  getClients(filter?: (client: IClientState) => boolean) {
    const states = this.clients.values();
    if (!filter) states;
    const results = new Set<IClientState>();
    for (const state of states) {
      if (filter?.(state)) results.add(state);
    }
    return results;
  }

  getClient(id: string) {
    return this.clients.get(id);
  }

  throwIfExists(id: string) {
    if (this.clients.has(id)) {
      throw new Error(`Client ID ${id} already exists`);
    }
  }

  validateClient(id: string, expectedType?: ClientType) {
    const metadata = this.clients.get(id);
    if (!metadata) {
      throw new Error(`Client with ID ${id} not found`);
    }
    if (expectedType && metadata.clientType !== expectedType) {
      throw new Error(`Client with ID ${id} is not a ${expectedType}`);
    }
  }

  isOperable(id: string, now: number) {
    const client = this.getClient(id);
    if (!client) return false;
    if (client.status == "lagging") return false;
    if (client.avgProcessingTime > this.processingTimeThresholdMs) return false;
    if (client.pendingAcks > this.pendingThresholdMs) return false;
    return now - client.lastActiveAt < this.inactivityThresholdMs;
  }

  isIdle(id: string) {
    const client = this.getClient(id);
    if (!client) return false;
    return client.status === "idle";
  }

  recordActivity(id: string, activityRecord: Partial<IClientState>) {
    if (!this.clients.has(id)) return;
    const client = this.clients.get(id)!;
    client.lastActiveAt = Date.now();

    for (let key in activityRecord) {
      if (typeof client[key] != "number") {
        client[key] = activityRecord[key];
      } else {
        client[key] = client[key] + activityRecord[key];
      }
    }

    if (client.messageCount > 0) {
      client.avgProcessingTime = client.processingTime / client.messageCount;
    }

    this.clients.set(id, client);
  }

  getMetadata() {
    let avgProcessingTime = 0;
    let producersCount = 0;
    let consumersCount = 0;
    let dlqConsumersCount = 0;
    let operableCount = 0;
    let idleCount = 0;
    const now = Date.now();

    const clients = this.clients.values();
    for (const client of clients) {
      avgProcessingTime += client.avgProcessingTime;
      if (this.isOperable(client.id, now)) operableCount++;
      if (this.isIdle(client.id)) idleCount++;
      if ((client.clientType = "producer")) producersCount++;
      else if ((client.clientType = "consumer")) consumersCount++;
      else dlqConsumersCount++;
    }

    return {
      count: this.clients.size,
      producersCount,
      consumersCount,
      dlqConsumersCount,
      operableCount,
      idleCount,
      avgProcessingTime,
    };
  }
}
interface IPublishResult {
  id: number;
  status: "success" | "error";
  ts: number;
  error?: string;
}
interface IProducer<Data> {
  publish(batch: Data[], metadata?: MetadataInput): Promise<IPublishResult[]>;
}
class Producer<Data> implements IProducer<Data> {
  constructor(
    private readonly publishingService: IPublishingService,
    private readonly messageFactory: IMessageFactory<Data>,
    private readonly topicName: string,
    private readonly id: number
  ) {}

  async publish(batch: Data[], metadata: MetadataInput = {}) {
    const results: IPublishResult[] = [];

    const messages = await this.messageFactory.create(batch, {
      ...metadata,
      topic: this.topicName,
      producerId: this.id,
    });

    for (const { message, meta, error } of messages) {
      const { id, ts } = meta;

      if (error) {
        const err = error instanceof Error ? error.message : "Unknown error";
        results.push({ id, ts, error: err, status: "error" });
        continue;
      }

      try {
        await this.publishingService.publish(this.id, message, meta);
        results.push({ id, ts, status: "success" });
      } catch (err) {
        const error = err instanceof Error ? err.message : "Unknown error";
        results.push({ id, ts, error, status: "error" });
      }
    }

    return results;
  }
}
interface IProducerFactory<Data> {
  create(id: number): IProducer<Data>;
}
class ProducerFactory<Data> implements IProducerFactory<Data> {
  private messageFactory: IMessageFactory<Data>;
  constructor(
    private readonly topicName: string,
    private readonly publishingService: IPublishingService,
    schemaRegistry: ISchemaRegistry,
    getTopicCapacity: () => number,
    codec: ICodec,
    schema?: string,
    maxMessageSize?: number,
    maxSizeBytes?: number
  ) {
    const validators: IMessageValidator<Data>[] = [];
    if (schema) {
      validators.push(new SchemaValidator(schemaRegistry, schema));
    }
    if (maxMessageSize) {
      validators.push(new SizeValidator(maxMessageSize));
    }
    if (maxSizeBytes) {
      validators.push(new CapacityValidator(maxSizeBytes, getTopicCapacity));
    }

    this.messageFactory = new MessageFactory<Data>(codec, validators);
  }

  create(id: number) {
    return new Producer(
      this.publishingService,
      this.messageFactory,
      this.topicName,
      id
    );
  }
}
interface IConsumerConfig {
  routingKeys?: string[];
  groupId?: string;
  limit?: number;
  autoAck?: boolean;
}
interface IConsumer<Data> {
  consume(): Promise<Data[]>;
  ack(messageId?: number): Promise<number[]>;
  nack(messageId?: number, requeue?: boolean): Promise<number>;
  subscribe(listener: ISubscriptionListener<Data>): void;
  unsubscribe(): void;
}
class Consumer<Data> implements IConsumer<Data> {
  private readonly limit: number;
  constructor(
    private readonly consumptionService: IConsumptionService<Data>,
    private readonly ackService: IAckService,
    private readonly subscriptionService: ISubscriptionService<Data>,
    private readonly id: number,
    private readonly autoAck = false,
    limit?: number
  ) {
    this.limit = Math.max(1, limit!);
  }

  async consume() {
    const messages: Data[] = [];

    for (let i = 0; i < this.limit; i++) {
      const message = await this.consumptionService.consume(
        this.id,
        this.autoAck
      );
      if (!message) break;
      messages.push(message);
    }

    return messages;
  }

  async ack(messageId?: number) {
    return this.ackService.ack(this.id, messageId);
  }

  async nack(messageId?: number, requeue = true): Promise<number> {
    return this.ackService.nack(this.id, messageId, requeue);
  }

  subscribe(listener: ISubscriptionListener<Data>): void {
    this.subscriptionService.subscribe(this.id, listener, this.autoAck);
  }

  unsubscribe(): void {
    this.subscriptionService.unsubscribe(this.id);
  }
}
interface IDLQConsumer<Data> {
  consume(): Promise<IDLQEntry<Data>[]>;
  replayDlq(
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ): Promise<number>;
}
class DLQConsumer<Data> implements IDLQConsumer<Data> {
  private readonly limit: number;
  private reader: AsyncGenerator<IDLQEntry<Data>, void, unknown>;
  constructor(
    private readonly dlqService: IDLQService<Data>,
    private readonly id: number,
    limit?: number
  ) {
    this.limit = Math.max(1, limit!);
    // singleton reader allows to read only once, waiting for the newest messages to arrive
    this.reader = this.dlqService.createDlqReader();
  }

  async consume() {
    const messages: IDLQEntry<Data>[] = [];

    for await (const message of this.reader) {
      if (!message) break;
      messages.push(message);
      if (messages.length == this.limit) break;
    }

    return messages;
  }

  async replayDlq(
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ) {
    return this.dlqService.replayDlq(this.id, handler, filter);
  }

  // subscribe(listener: (message: IDLQEntry<Data>) => Promise<void>): void {
  //   this.topic.subscribe<IDLQEntry<Data>>(this.id, listener);
  // }

  // unsubscribe(): void {
  //   this.topic.unsubscribe(this.id);
  // }
}
interface IClientManagementService<Data> {
  createProducer(): IProducer<Data>;
  createConsumer(config?: IConsumerConfig): IConsumer<Data>;
  createDLQConsumer(limit?: number): DLQConsumer<Data>;
  deleteClient(id: number): void;
  getMetadata(): {
    count: number;
    producersCount: number;
    consumersCount: number;
    dlqConsumersCount: number;
    operableCount: number;
    idleCount: number;
    avgProcessingTime: number;
  };
}
class ClientManagementService<Data> implements IClientManagementService<Data> {
  constructor(
    private readonly producerFactory: IProducerFactory<Data>,
    private readonly clientManager: IClientManager,
    private readonly messageRouter: IMessageRouter,
    private readonly queueManager: IQueueManager,
    private readonly consumptionService: IConsumptionService<Data>,
    private readonly ackService: IAckService,
    private readonly subscriptionService: ISubscriptionService<Data>,
    private readonly dlqService: IDLQService<Data>,
    private readonly logger?: ILogCollector
  ) {}

  createProducer(id = uniqueIntGenerator()): IProducer<Data> {
    this.clientManager.throwIfExists(id);
    this.clientManager.addClient("producer", id);
    this.logger?.log(`producer_created`, { id });

    return this.producerFactory.create(id);
  }

  createConsumer(
    id = uniqueIntGenerator(),
    config: IConsumerConfig = {}
  ): IConsumer<Data> {
    this.clientManager.throwIfExists(id);
    const { groupId, routingKeys, autoAck, limit } = config;
    this.messageRouter.addConsumer(id, groupId, routingKeys);
    this.clientManager.addClient("consumer", id);
    this.queueManager.addQueue(id);

    this.logger?.log(`consumer_created`, { id });

    return new Consumer(
      this.consumptionService,
      this.ackService,
      this.subscriptionService,
      id,
      autoAck,
      limit
    );
  }

  createDLQConsumer(
    id = uniqueIntGenerator(),
    limit?: number
  ): DLQConsumer<Data> {
    this.clientManager.throwIfExists(id);
    this.clientManager.addClient("dlq_consumer", id);
    this.logger?.log(`dlq_consumer_created`, { id });

    return new DLQConsumer(this.dlqService, id, limit);
  }

  deleteClient(id: number) {
    this.messageRouter.removeConsumer(id);
    this.clientManager.removeClient(id);
    this.queueManager.removeQueue(id);

    this.logger?.log("client_deleted", { id });
  }

  getMetadata() {
    return this.clientManager.getMetadata();
  }
}
//
//
//
// SRC/TOPIC.TS
interface IMetricsCollector {
  recordEnqueue(byteSize: number, latencyMs: number): void;
  recordDequeue(latencyMs: number): void;
  getMetrics(): {
    ts: number;
    totalMessagesPublished: number;
    totalBytes: number;
    depth: number;
    enqueueRate: number;
    dequeueRate: number;
    avgLatencyMs: number;
  };
}
class TopicMetricsCollector implements IMetricsCollector {
  private totalMessagesPublished = 0;
  private totalBytes = 0;
  private ts = Date.now();
  private depth = 0;
  private enqueueRate = 0;
  private dequeueRate = 0;
  private avgLatencyMs = 0; // Time in queue

  recordEnqueue(byteSize: number, latencyMs: number): void {
    this.totalMessagesPublished++;
    this.totalBytes += byteSize;
    this.depth += 1;
    this.enqueueRate += 1;
    this.updateAvgLatency(latencyMs);
  }

  recordDequeue(latencyMs: number): void {
    this.depth -= 1;
    this.dequeueRate += 1;
    this.updateAvgLatency(latencyMs);
  }

  private updateAvgLatency(latencyMs: number) {
    this.avgLatencyMs = this.avgLatencyMs * 0.9 + latencyMs * 0.1; // Exponential moving average
  }

  getMetrics() {
    return {
      ts: this.ts,
      totalMessagesPublished: this.totalMessagesPublished,
      totalBytes: this.totalBytes,
      depth: this.depth,
      enqueueRate: this.enqueueRate,
      dequeueRate: this.dequeueRate,
      avgLatencyMs: this.avgLatencyMs,
    };
  }
}
interface ITopicConfig {
  schema?: string; // registered schema` name
  persistThresholdMs?: number; // flush delay, 1000 default, if need ephemeral set Infinity
  retentionMs?: number; // 86_400_000 1 day
  maxSizeBytes?: number;
  maxDeliveryAttempts?: number;
  maxMessageSize?: number;
  ackTimeoutMs?: number; // e.g., 30_000
  consumerInactivityThresholdMs?: number; // 600_000;
  consumerProcessingTimeThresholdMs?: number;
  consumerPendingThresholdMs?: number;
  // maxUnackedMessagesPerConsumer?: number;
  // partitions?: number;
  // archivalThresholdMs?: number; // 100_000
}
interface ITopic<Data> {
  name: string;
  config: ITopicConfig;
  createProducer(): IProducer<Data>;
  createConsumer(config: IConsumerConfig): IConsumer<Data>;
  createDLQConsumer(limit?: number): DLQConsumer<Data>;
  deleteClient(id: number): void;
  getMetadata(): {
    name: string;
    ts: number;
    totalMessagesPublished: number;
    totalBytes: number;
    depth: number;
    enqueueRate: number;
    dequeueRate: number;
    avgLatencyMs: number;
    queuedMessages: {
      size: number;
    };
    subscriptions: {
      count: number;
    };
    pendingAcks: {
      count: number;
    };
    dlq: {
      size: number;
    };
    router: {
      consumerGroups: {
        name: string;
        count: number;
      }[];
    };
    delayedMessages: {
      count: number;
    };
    clients: {
      count: number;
      producersCount: number;
      consumersCount: number;
      dlqConsumersCount: number;
      operableCount: number;
      avgProcessingTime: number;
    };
  };
}
class Topic<Data> implements ITopic<Data> {
  constructor(
    public readonly name: string,
    public readonly config: ITopicConfig,
    private readonly publishingService: IPublishingService,
    private readonly consumptionService: IConsumptionService<Data>,
    private readonly subscriptionService: ISubscriptionService<Data>,
    private readonly ackService: IAckService,
    private readonly dlqService: IDLQService<Data>,
    private readonly clientService: IClientManagementService<Data>,
    private readonly metrics: IMetricsCollector
  ) {}

  getMetadata() {
    return {
      name: this.name,
      subscriptions: this.subscriptionService.getMetadata(),
      clients: this.clientService.getMetadata(),
      dlq: this.dlqService.getMetadata(),
      ...this.ackService.getMetadata(),
      ...this.publishingService.getMetadata(),
      ...this.consumptionService.getMetadata(),
      ...this.metrics.getMetrics(),
    };
  }

  createProducer() {
    return this.clientService.createProducer();
  }

  createConsumer(config: IConsumerConfig) {
    return this.clientService.createConsumer(config);
  }

  createDLQConsumer(limit?: number) {
    return this.clientService.createDLQConsumer(limit);
  }

  deleteClient(id: number) {
    return this.clientService.deleteClient(id);
  }

  async dispose() {}
}
interface ITopicFactory {
  create<Data>(name: string, config?: Partial<ITopicConfig>): Topic<Data>;
}
class TopicFactory implements ITopicFactory {
  constructor(
    private schemaRegistry: ISchemaRegistry,
    private defaultConfig: ITopicConfig = {
      persistThresholdMs: 1000,
      retentionMs: 86_400_000, // 1 day
      maxDeliveryAttempts: 5,
      maxUnackedMessagesPerConsumer: 10,
    },
    private codecFactory: new () => ICodec = ThreadedBinaryCodec,
    private queueFactory: new () => IPriorityQueue = HighCapacityBinaryHeapPriorityQueue,
    private storageFactory: new (
      ...args
    ) => IMessageStorage<unknown> = LevelDBMessageStorage,
    private logService?: LogService
  ) {}

  create<Data>(name: string, config?: Partial<ITopicConfig>): Topic<Data> {
    const mergedConfig = { ...this.defaultConfig, ...config };

    this.validateTopicName(name);
    const codec = new this.codecFactory();
    const logger = this.logService?.forTopic(name);

    // Build modules
    const messageStorage = new this.storageFactory(
      name,
      codec,
      mergedConfig.retentionMs,
      mergedConfig.persist,
      mergedConfig.persistThresholdMs
    );
    const metrics = new TopicMetricsCollector();
    const clientManager = new ClientManager(
      mergedConfig.consumerInactivityThresholdMs,
      mergedConfig.consumerProcessingTimeThresholdMs,
      mergedConfig.consumerPendingThresholdMs
    );
    const queueManager = new QueueManager(this.queueFactory);
    const dlqManager = new DLQManager<Data>(name, messageStorage, logger);
    const deliveryCounter = new DeliveryCounter(messageStorage, metrics);

    const pendingAcks = new PendingAcks(
      clientManager,
      mergedConfig.maxUnackedMessagesPerConsumer
    );
    const subscriptionManager = new SubscriptionManager<Data>(
      clientManager,
      queueManager,
      messageStorage,
      pendingAcks,
      deliveryCounter,
      logger
    );

    const messageRouter = new MessageRouter<Data>(
      clientManager,
      queueManager,
      subscriptionManager,
      dlqManager
    );
    const delayedManager = new DelayedMessageManager<Data>(
      new DelayScheduler<number>(new this.queueFactory()),
      messageStorage,
      messageRouter,
      deliveryCounter,
      logger
    );
    const pipeline = new PipelineFactory<Data>().create(
      dlqManager,
      delayedManager,
      mergedConfig?.maxDeliveryAttempts
    );
    const ackMonitor = new AckMonitor(pendingAcks, mergedConfig?.ackTimeoutMs);

    // Build services
    const publishingService = new PublishingService(
      messageStorage,
      pipeline,
      messageRouter,
      deliveryCounter,
      clientManager,
      delayedManager,
      metrics,
      logger
    );
    const consumptionService = new ConsumptionService(
      queueManager,
      messageStorage,
      pendingAcks,
      deliveryCounter,
      clientManager,
      logger
    );
    const ackService = new AckService(
      pendingAcks,
      deliveryCounter,
      ackMonitor,
      messageStorage,
      pipeline,
      queueManager,
      subscriptionManager,
      logger
    );
    const subscriptionService = new SubscriptionService<Data>(
      name,
      subscriptionManager,
      logger
    );
    const dlqService = new DLQService(dlqManager, clientManager, logger);
    const producerFactory = new ProducerFactory(
      name,
      publishingService,
      this.schemaRegistry,
      () => metrics.getMetrics().totalBytes,
      codec,
      mergedConfig.schema,
      mergedConfig.maxMessageSize,
      mergedConfig.maxSizeBytes
    );
    const clientManagementService = new ClientManagementService(
      producerFactory,
      clientManager,
      messageRouter,
      queueManager,
      consumptionService,
      ackService,
      subscriptionService,
      dlqService,
      logger
    );

    return new Topic<Data>(
      name,
      publishingService,
      consumptionService,
      subscriptionService,
      ackService,
      dlqService,
      clientManagementService,
      metrics
    );
  }

  private validateTopicName(name: string): void {
    if (!name || !/^[a-zA-Z0-9_-]+$/.test(name)) {
      throw new Error(
        "Invalid topic name. Use alphanumeric, underscore and hyphen characters."
      );
    }
  }
}
//
//
//
// ROOT
interface ILogger {
  info(msg: string, extra?: unknown): void;
  warn(msg: string, extra?: unknown): void;
  error(msg: string, extra?: unknown): void;
  debug?(msg: string, extra?: unknown): void;
}
interface ILogCollector {
  log(msg: string, extra?: object, level?: keyof ILogger): void;
  flush: () => void;
  destroy(): void;
}
class LogCollector implements ILogCollector {
  private flushId?: number;
  private buffer = new Set<[string, object, keyof ILogger]>();

  constructor(
    private logger: ILogger,
    private chunkSize = 50,
    private topic?: string
  ) {}

  log(msg: string, extra?: object, level: keyof ILogger = "info") {
    this.buffer.add([msg, extra ?? {}, level]);
    this.scheduleFlush();
  }

  private scheduleFlush() {
    this.flushId ??= setImmediate(this.flush);
  }

  flush = () => {
    this.flushId = undefined;
    let count = 0;
    const ts = Date.now();
    const { topic } = this;

    for (const entry of this.buffer) {
      if (count++ >= this.chunkSize) break;
      const [message, extra, level] = entry;
      this.logger[level]?.(message, Object.assign(extra, { topic, ts }));
      this.buffer.delete(entry);
    }

    if (this.buffer.size > 0) {
      this.scheduleFlush();
    }
  };

  destroy() {
    clearImmediate(this.flushId);
    this.flushId = undefined;
    this.buffer = new Set();
  }
}
interface ILogService {
  globalCollector: ILogCollector;
  forTopic(name: string): ILogCollector;
  flushAll(): void;
}
class LogService implements ILogService {
  private topicCollectors = new Map<string, ILogCollector>();
  public globalCollector: ILogCollector;

  constructor(
    private logger: ILogger,
    private config: { bufferSize?: number } = {}
  ) {
    this.globalCollector = new LogCollector(logger, config.bufferSize);
  }

  forTopic(name: string): ILogCollector {
    if (!this.topicCollectors.has(name)) {
      this.topicCollectors.set(
        name,
        new LogCollector(this.logger, this.config.bufferSize, name)
      );
    }
    return this.topicCollectors.get(name)!;
  }

  flushAll(): void {
    this.globalCollector.flush();
    this.topicCollectors.forEach((collector) => collector.flush());
  }
}
interface ISchemaRegistry extends ISerializable {
  register<Data>(name: string, schema: JSONSchemaType<Data>): string;
  getValidator(schema: string): ((data: any) => boolean) | undefined;
  remove(schema: string): void;
}
class SchemaRegistry implements ISchemaRegistry {
  // TODO: persist
  private validators: PersistedMap<string, Map<number, (data: any) => boolean>>;
  private ajv: Ajv;

  constructor(mapFactory: PersisteMapFactory, options?: Ajv.Options) {
    this.ajv = new Ajv({
      allErrors: true,
      coerceTypes: false,
      useDefaults: true,
      code: { optimize: true, esm: true },
      ...options,
    });
    this.validators = mapFactory.create("validators", this);
  }

  serialize(validatorMap: Map<number, (data: any) => boolean>) {}

  desialize() {}

  register<Data>(name: string, schema: JSONSchemaType<Data>): string {
    if (!this.validators.has(name)) {
      this.validators.set(name, new Map());
    }
    const validators = this.validators.get(name)!;
    const version = validators.size + 1;
    validators.set(version, this.ajv.compile(schema));
    return `${name}:${version}`;
  }

  getValidator(schema: string): ((data: any) => boolean) | undefined {
    let version = +schema.split(":")[1];
    if (Number.isNaN(version) || version === 0) version = 1;

    return this.validators.get(schema)?.get(version);
  }

  remove(schema: string): void {
    this.validators.delete(schema);
  }
}
class TopicRegistry {
  private topics: PersistedMap<string, ITopic<any>>;
  constructor(
    mapFactory: PersisteMapFactory,
    private topicFactory: ITopicFactory,
    private logService?: ILogService
  ) {
    this.topics = mapFactory.create("topics", this);
  }

  serialize(topic: ITopic<any>) {
    const { name, config } = topic;
    return { name, config };
  }

  deserialize(data: { name: string; config: ITopicConfig }) {
    return this.topicFactory.create(data.name, data.config);
  }

  create<Data>(name: string, config: ITopicConfig): ITopic<Data> {
    if (this.topics.has(name)) {
      throw new Error("Topic already exists");
    }

    const topic = this.topicFactory.create<Data>(name, config);
    this.topics.set(name, topic);

    this.logService?.globalCollector.log("Topic created", {
      ...config,
      name,
    });

    return topic;
  }

  list() {
    return this.topics.keys();
  }

  get(name: string): ITopic<any> | undefined {
    if (!this.topics.has(name)) throw new Error("Topic not found");
    return this.topics.get(name);
  }

  delete(name: string): void {
    this.get(name);
    this.topics.delete(name);

    this.logService?.globalCollector.log("Topic deleted", { name });
  }
}

// make all deletable/disposable
// custom errs: new ValidationError()
// project structure

// separation of conserns, ioc(inversify_js), visualize components and flows
// At-least-once/Exactly-once: Your system already supports At-least-once delivery , which is a prerequisite for Exactly-once. These features ensure that no message is lost, but they do not prevent duplicates.
// Allow groups to use round-robin, sticky sessions, etc., not just hash rings.
// switch to Standalone server: you hit >50k msg/sec, cross-service/multi-lang support => need protobuf & lib/sdk per lang

// class ProtobufCodec implements ICodec {
//   encode<Data>(message: Message<Data>): Buffer {
//     const protoMsg = ProtoMessage.create({
//       data: this.serializeData(message.data),
//       // Convert all numbers to bigint
//       ts: BigInt(message.ts),
//       attempts: BigInt(message.attempts),
//       priority: message.priority ? BigInt(message.priority) : message.priority,
//       ttl: message.ttl ? BigInt(message.ttl) : message.ttl,
//       ttd: message.ttd ? BigInt(message.ttd) : message.ttd,
//       batchIdx: message.batchIdx ? BigInt(message.batchIdx) : message.batchIdx,
//       batchSize: message.batchSize
//         ? BigInt(message.batchSize)
//         : message.batchSize,
//     });
//     return ProtoMessage.encode(protoMsg).finish();
//   }

//   decode<Data>(bytes: Buffer): Message<Data> {
//     const protoMsg = ProtoMessage.decode(bytes);
//     return Object.assign(new Message(), {
//       data: this.deserializeData<Data>(protoMsg.data),
//       // Convert back to JS numbers
//       ts: Number(protoMsg.ts),
//       attempts: Number(protoMsg.attempts),
//       priority: protoMsg.priority
//         ? Number(protoMsg.priority)
//         : protoMsg.priority,
//       ttl: protoMsg.ttl ? Number(protoMsg.ttl) : protoMsg.ttl,
//       ttd: protoMsg.ttd ? Number(protoMsg.ttd) : protoMsg.ttd,
//       batchIdx: protoMsg.batchIdx
//         ? Number(protoMsg.batchIdx)
//         : protoMsg.batchIdx,
//       batchSize: protoMsg.batchSize
//         ? Number(protoMsg.batchSize)
//         : protoMsg.batchSize,
//     });
//   }

//   private serializeData<T>(data: T): Buffer {
//     // no data schema fallback - just binary
//     return Buffer.from(JSON.stringify(data));
//   }

//   private deserializeData<T>(bytes: Buffer): T {
//     // no data schema fallback - just binary
//     return JSON.parse(Buffer.toString(bytes));
//   }
// }

// class JSONCodec implements ICodec {
//   encode<T>(data: T, meta: MessageMetadata) {
//     try {
//       // 1. Build the complete object
//       const message = { "0": data };
//       meta.keys.forEach((k, i) => {
//         if (meta[k] !== undefined) message[i + 1] = meta[k];
//       });

//       // 2. Pre-allocation reduces GC pressure
//       const jsonString = JSON.stringify(message);
//       const buffer = Buffer.allocUnsafe(Buffer.byteLength(jsonString));

//       // 3. Single write operation
//       buffer.write(jsonString, 0, "utf8");
//       return buffer;
//     } catch (e) {
//       throw new Error("Failed to encode message");
//     }
//   }

//   decode<T>(buffer: Buffer): [T, MessageMetadata] {
//     try {
//       // 1. Fast path for empty/small buffers
//       if (buffer.length < 2) throw new Error("Invalid message");

//       // 2. Single string conversion
//       const str =
//         buffer.length < 4096
//           ? buffer.toString("utf8") // Small buffers
//           : Buffer.prototype.toString.call(buffer, "utf8"); // Large buffers avoids prototype lookup

//       // 3. Parse with reviver for direct metadata mapping
//       const meta = new MessageMetadata();
//       const data = JSON.parse(str, (k, v: number | string) => {
//         if (k === "0") return v; // Return payload as-is
//         const metaIndex = parseInt(k, 10) - 1;
//         if (!isNaN(metaIndex)) {
//           const metaKey = meta.keys[metaIndex];
//           // @ts-ignore
//           if (metaKey) meta[metaKey] = v;
//         }
//         return;
//       }) as T;

//       return [data, meta];
//     } catch (e) {
//       throw new Error("Failed to decode message");
//     }
//   }
// }

// const logger = pino({
//   level: process.env.LOG_LEVEL || "info",
//   transport:
//     process.env.NODE_ENV === "development"
//       ? { target: "pino-pretty" }
//       : undefined,
//   base: { service: "message-broker" },
// });

//
// const topicDepth = new Counter({
//   name: "queue_depth",
//   help: "Current messages in queue",
//   labelNames: ["topic"],
// });
// const producerMessageCount = new Counter({
//   name: "producer_messages_total",
//   help: "Total messages sent by producer",
//   labelNames: ["topic", "producer_id"],
// });
// const consumerLag = new Gauge({
//   name: "consumer_pending_messages",
//   help: "Pending messages per consumer",
//   labelNames: ["topic", "consumer_id"],
// });
// class MetricsExporter {
//   private metricsProviders = new Map<string, () => Record<string, number>>();
//   register(topicName: string, getMetrics: () => Record<string, number>) {
//     this.metricsProviders.set(topicName, getMetrics);
//   }
//   start() {
//     setInterval(() => {
//       this.metricsProviders.forEach((getMetrics, topicName) => {
//         const lags = getMetrics(); // E.g., { "consumer-1": 5, "consumer-2": 10 }
//         Object.entries(lags).forEach(([consumerId, lag]) => {
//           consumerLagGauge.set({ topic: topicName, consumerId }, lag);
//         });
//       });
//     }, 15_000);
//   }
// }

//           - leveldb: 300k (space:78mb, w:18s, r:2.8s, d:ok)
//           - rocksd: 300k (space:100mb, w:15.3s, r:2.9s, d:faster, +ttl, +backups, +mcore, +transaction?, partitions(Column Families allow to group related data together and store it in a separate partition))
//           - Redis(aof) use more disk space, partialy already has broker capabilities(pubsub, streams) which also in ram so i double ram usage, has many other features which i dont need
//             redis list[buffer from protobuf] for queues and redis stream for broadcast(eventlog)

// Job queues and eventLog have distinct arch and seems like this lib should be doing one thing eventually.
// I think i dont want kafka streaming. I just want a scallable message broker which is more like Rabbitmq i guess.
// RabbitMq analogue: log is an exchange, offsets are binary queues(with messageId if log is map or messageIndex if log is list) binded to exchange but ram effective
// message marks will be deleted from the offsets on ack, if nack - also offset manipulation
// but we need both routing and consistency hashing (correlationId) implementation here
// Here is my proposal for universal Topic:
// 1. all messages should be stored in kv log (map/leveldb) and deleted only with retention. Can we skip wal if log persisted?
// 2. we will use virtual offsets per consumer but it will be buffer with messageid(or messageIndex if log is list)
// on message ack only offset will be deleted, on nack also offset manipulation - message already in log

//        1. AJV for per topic json schema validation (2x faster zod due precompilation and faster than protobuf validation)
//        2. Custom binary packing codec based on Buffer, fixed structure(metadata), bitflags for optional fields (no redundant data), precomputed Offsets.
//           Buffer.from is 30% faster TextEncode.encode due the buffer preallocation.
//           Uses worker_threads(makes 2x slower but offload main thread).
//           Codec provides max speed, min size/resource_usage within nodejs specific impl.
//           But for a standalone server its need to be switch to protobuf(encode + compress(indexes vs keys) + schema validation) or message_pack(encode any type + compress)
//           Metrics
//             - json: encode(12k m/sec), decode(9k m/sec), size(200b), gc/cpu(high)
//             - this impl: encode(40k m/sec), decode(50k m/sec), size(60b), gc/cpu(low)
//             - protobuf: encode(30k m/sec), decode(35k m/sec), size(80b), gc/cpu(medium)
//             - message_pack: encode(16k m/sec), decode(15k m/sec), size(100b), gc/cpu(medium)
//        3. leveldb(+snappy)/rocksdb cpp addon
//           - compressor comparision: gzip(zlib wrapper, 4x but slow)/brotli(+20% zlib, slow) => ZSTD(between gzip & snappy) => snappy(only 2x but fast)