import type { Options as AjvOptions, JSONSchemaType } from "ajv";
import Ajv from "ajv";
import crc from "crc-32";
import fs from "fs/promises";
import { Level } from "level";
import { clearImmediate, setImmediate } from "node:timers";
import path from "path";
import snappy from "snappy";
import { BinaryHeapPriorityQueue } from "./binary_heap_priority_queue";
import type { ICodec } from "./codec/binary_codec";
import { ThreadedBinaryCodec } from "./codec/binary_codec.threaded";
import {
  InMemoryHashRing,
  SHA256HashService,
  type IHashRing,
} from "./hash_ring";
import { uniqueIntGenerator } from "./utils";
import { Mutex } from "./mutex";
import { validateSchema } from "json-schema-compatibility";
//
//
//
// PERSISTENT_STRUCTURE
interface IFlushManager {
  register(task: () => Promise<void>): void;
  unregister(task: () => Promise<void>): void;
  commit(): void;
  flush: () => Promise<void>;
  size(): number;
  stop(): void;
}
class FlushManager implements IFlushManager {
  private flushes: Array<() => Promise<void>> = [];
  private pendingCounter = 0;
  private timer?: NodeJS.Timeout;

  constructor(
    private persistThresholdMs = 1000,
    private maxPendingFlushes = 100,
    private memoryPressureThresholdMB = 1024
  ) {
    this.init();
  }

  stop() {
    clearInterval(this.timer);
    this.timer = undefined;
    this.flush();
  }

  register(task: () => Promise<void>) {
    this.flushes.push(task);
  }

  unregister(task: () => Promise<void>): void {
    const idx = this.flushes.indexOf(task);
    if (idx === -1) return;
    this.flushes.splice(idx, 1);
  }

  commit() {
    if (++this.pendingCounter < this.maxPendingFlushes) return;
    if (!this.isMemoryPressured()) return;
    this.flush();
  }

  size() {
    return this.flushes.length;
  }

  flush = async () => {
    if (!this.pendingCounter) return;
    this.pendingCounter = 0;
    await Promise.all(this.flushes);
  };

  private init() {
    if (this.persistThresholdMs === Infinity) return;
    this.timer = setInterval(
      this.flush,
      Math.max(this.persistThresholdMs, 100)
    );
    process.on("beforeExit", this.stop);
  }

  private isMemoryPressured() {
    const { rss } = process.memoryUsage();
    const usedMB = Math.round(rss / 1024 / 1024);
    return usedMB > this.memoryPressureThresholdMB;
  }
}
export interface ISerializable<T = unknown, R = unknown, K = string | number> {
  serialize(data: T, key: K): R;
  deserialize(data: R, key: K): T;
}
abstract class PersistedStructure {
  protected mutex = new Mutex();
  protected isCleared = false;

  constructor(
    protected db: Level<string, Buffer>,
    protected namespace: string,
    protected codec: ICodec,
    protected flushManager?: IFlushManager,
    protected logger?: ILogCollector
  ) {
    this.flushManager?.register(this.flush);
    this.init();
  }

  protected async init() {
    try {
      for await (const [k, v] of this.db.iterator({
        gt: `${this.namespace}!`,
        lt: `${this.namespace}~`,
      })) {
        const key = k.slice(this.namespace.length + 1);
        await this.restoreItem(key, v);
      }
    } catch (error) {
      this.logger?.log(
        `Failed to restore ${this.namespace}`,
        { error },
        "error"
      );
    }
  }

  protected abstract evictIfFull(): void;
  protected abstract isFlushSkipped(): boolean;
  protected abstract flushCleanup(): void;
  protected abstract flushIterator(): Generator<
    readonly [string | number, any],
    void,
    unknown
  >;
  protected abstract restoreItem(
    key: string | number,
    value: Buffer
  ): Promise<void>;

  flush = async () => {
    if (this.isFlushSkipped()) return;
    await this.mutex.acquire();

    try {
      if (this.isCleared) {
        await this.db.clear({
          gt: `${this.namespace}!`,
          lt: `${this.namespace}~`,
        });

        this.isCleared = false;
        this.flushCleanup();
        return;
      }

      const batch = this.db.batch();
      const encodePromises: Promise<void>[] = [];

      for (const [key, value] of this.flushIterator()) {
        if (value !== undefined) {
          encodePromises.push(
            this.codec.encode(value).then((encoded) => {
              batch.put(`${this.namespace}!${key}`, encoded);
            })
          );
        } else {
          batch.del(`${this.namespace}!${key}`);
        }
      }

      await Promise.all(encodePromises);
      await batch.write();
      this.flushCleanup();
    } catch (error) {
      this.logger?.log(`Failed to flush ${this.namespace}`, { error }, "error");
    } finally {
      this.mutex.release();
    }
  };

  stop(): void {
    this.flushManager?.unregister(this.flush);
  }
}
export interface IPersistedMap<K, V> {
  flush(): Promise<void>;
  has(key: K): boolean;
  get(key: K): V | undefined;
  set(key: K, value: V): Map<K, V>;
  delete(key: K): boolean;
  clear(): void;
  size: number;
  keys(): MapIterator<K>;
  values(): MapIterator<V>;
  entries(): MapIterator<[K, V]>;
}
class PersistedMap<K extends string | number, V>
  extends PersistedStructure
  implements IPersistedMap<K, V>
{
  private map = new Map<K, V>();
  private dirtyKeys = new Set<K>();

  constructor(
    db: Level<string, Buffer>,
    namespace: string,
    codec: ICodec,
    flushManager?: IFlushManager,
    logger?: ILogCollector,
    private valueSerializer?: ISerializable<V>,
    private maxSize = Infinity
  ) {
    super(db, namespace, codec, flushManager, logger);
  }

  has(key: K) {
    return this.map.has(key);
  }

  get(key: K) {
    return this.map.get(key);
  }

  set(key: K, value: V) {
    // works for primitives, non-primitive will always return true which is also ok
    if (this.map.get(key) !== value) {
      this.dirtyKeys.add(key);
      this.flushManager?.commit();
    }
    this.map.set(key, value);
    this.evictIfFull();
    return this.map;
  }

  delete(key: K): boolean {
    this.dirtyKeys.add(key);
    this.flushManager?.commit();
    return this.map.delete(key);
  }

  clear(): void {
    this.isCleared = true;
    this.map.clear();
    this.flush();
  }

  get size(): number {
    return this.map.size;
  }

  keys() {
    return this.map.keys();
  }

  values() {
    return this.map.values();
  }

  entries() {
    return this.map.entries();
  }

  override evictIfFull(): void {
    if (this.map.size >= this.maxSize) {
      const firstKey = this.map.keys().next().value!;
      this.map.delete(firstKey);
    }
  }

  override async restoreItem(key: K, value: Buffer): Promise<void> {
    const decoded = await this.codec.decode<V>(value);
    this.map.set(
      key,
      this.valueSerializer?.deserialize(decoded, key as K) ?? decoded
    );
  }

  override isFlushSkipped(): boolean {
    return this.dirtyKeys.size === 0;
  }

  override flushCleanup() {
    this.dirtyKeys.clear();
  }

  override *flushIterator() {
    for (const key of this.dirtyKeys) {
      const value = this.map.get(key);
      if (value == undefined) {
        yield [key, undefined] as const;
      } else {
        yield [
          key,
          this.valueSerializer?.serialize(value, key) ?? value,
        ] as const;
      }
    }
  }
}
export interface IPersistedMapFactory {
  create<K extends string | number, V>(
    name: string,
    valueSerializer?: ISerializable<V>
  ): PersistedMap<K, V>;
}
class PersistedMapFactory implements IPersistedMapFactory {
  constructor(
    private db: Level<string, Buffer>,
    private codec: ICodec,
    private maxSize = Infinity,
    private flushManager?: IFlushManager,
    private logger?: ILogCollector
  ) {}

  create<K extends string | number, V>(
    name: string,
    valueSerializer?: ISerializable<V>
  ) {
    return new PersistedMap<K, V>(
      this.db,
      name,
      this.codec,
      this.flushManager,
      this.logger,
      valueSerializer,
      this.maxSize
    );
  }
}
interface IPersistedQueue<T> extends IPriorityQueue<T> {
  flush(): Promise<void>;
  clear(): void;
}
class PersistedQueue<T, K extends string | number>
  extends PersistedStructure
  implements IPersistedQueue<T>
{
  private pendingUpdates = new Map<K, [T, number] | undefined>();

  constructor(
    db: Level<string, Buffer>,
    public namespace: string,
    codec: ICodec,
    private queue: IPriorityQueue<T>,
    private keyRetriever: (entry: T) => K,
    flushManager?: IFlushManager,
    logger?: ILogCollector,
    private valueSerializer?: ISerializable<T>,
    private maxSize = Infinity
  ) {
    super(db, namespace, codec, flushManager, logger);
  }

  enqueue(value: T, priority = 0): void {
    this.queue.enqueue(value, priority);
    const key = this.keyRetriever(value);
    this.pendingUpdates.set(key, [value, priority]);
    this.flushManager?.commit();
    this.evictIfFull();
  }

  dequeue(): T | undefined {
    const data = this.queue.dequeue();
    if (data) {
      const key = this.keyRetriever(data);
      this.pendingUpdates.set(key, undefined);
      this.flushManager?.commit();
    }

    return data;
  }

  clear() {
    this.isCleared = true;
    this.flush();
  }

  size(): number {
    return this.queue.size();
  }

  peek() {
    return this.queue.peek();
  }

  isEmpty(): boolean {
    return this.queue.isEmpty();
  }

  override evictIfFull(): void {
    // LRU-style eviction (simplified)
    if (this.queue.size() < this.maxSize) return;
    this.dequeue();
  }

  override async restoreItem(key: K, value: Buffer): Promise<void> {
    const [rawData, priority] = await this.codec.decode<[T, number]>(value);
    this.queue.enqueue(
      this.valueSerializer?.deserialize(rawData, key) ?? rawData,
      priority
    );
  }

  override isFlushSkipped(): boolean {
    return this.pendingUpdates.size === 0;
  }

  override flushCleanup() {
    this.pendingUpdates.clear();
  }

  override *flushIterator() {
    for (const [key, value] of this.pendingUpdates) {
      if (value == undefined) {
        yield [key, undefined] as const;
      } else {
        const [data, priority] = value;
        yield [
          key,
          [this.valueSerializer?.serialize(data, key) ?? data, priority],
        ] as const;
      }
    }
  }
}
interface IPersistedQueueFactory {
  create<T, K extends string | number>(
    name: string,
    keyRetriever: (entry: T) => K,
    valueSerializer?: ISerializable<T>
  ): PersistedQueue<T, K>;
}
class PersistedQueueFactory implements IPersistedQueueFactory {
  constructor(
    private queueFactory: new () => IPriorityQueue<any>,
    private db: Level<string, Buffer>,
    private codec: ICodec,
    private maxSize = Infinity,
    private flushManager?: IFlushManager,
    private logger?: ILogCollector
  ) {}

  create<T, K extends string | number>(
    name: string,
    keyRetriever: (entry: T) => K,
    valueSerializer?: ISerializable<T>
  ) {
    return new PersistedQueue<T, K>(
      this.db,
      name,
      this.codec,
      new this.queueFactory(),
      keyRetriever,
      this.flushManager,
      this.logger,
      valueSerializer,
      this.maxSize
    );
  }
}
class MapSerializer<K, V> implements ISerializable {
  serialize(map: Map<K, V>): [K, V][] {
    return Array.from(map);
  }
  deserialize(data: [K, V][]): Map<K, V> {
    return new Map<K, V>(data);
  }
}
class PersistedQueueSerializer<T> implements ISerializable {
  constructor(
    private queueFactory: IPersistedQueueFactory,
    private keyRetriever: (entry: T) => string | number
  ) {}
  serialize(_: unknown, key: string) {
    return key;
  }
  deserialize(name: string) {
    return this.queueFactory.create(`queue!${name}`, this.keyRetriever);
  }
}
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
  // batchId?: number; // 4 bytes
  // batchIdx?: number; // 2 bytes
  // batchSize?: number; // 2 bytes
  // attempts: number = 1; // 1 byte // !!!!
  // consumedAt?: number; // 8 bytes. // !!!!
  // size: number = 0;
  // needAcks: number = 0;

  // Variable-width fields
  topic: string = "";
  correlationId?: string;
  routingKey?: string;
  dedupId?: string; // new

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
  validate(data: { data: Data; size: number; dedupId?: string }): void;
}
class DeduplicationValidator<Data> implements IMessageValidator<Data> {
  constructor(private deduplicationTracker: IDeduplicationTracker) {}

  validate({ dedupId }: { dedupId?: string }) {
    if (!dedupId) return;
    if (this.deduplicationTracker.has(dedupId)) {
      throw new Error("Message was already published");
    }

    this.deduplicationTracker.add(dedupId);
  }
}
class SchemaValidator<Data> implements IMessageValidator<Data> {
  constructor(
    private schemaRegistry: ISchemaRegistry,
    private schema: string
  ) {}

  validate({ data }: { data: Data }): void {
    const validator = this.schemaRegistry.getValidator(this.schema);
    if (!!validator && !validator(data)) {
      // @ts-ignore
      throw new Error(validator.errors);
    }
  }
}
class SizeValidator implements IMessageValidator<any> {
  constructor(private maxSize: number) {}

  validate({ size }: { size: number }) {
    if (size > this.maxSize) throw new Error("Message too large");
  }
}
class CapacityValidator implements IMessageValidator<any> {
  constructor(
    private topicMaxCapacity: number,
    private getTopicCapacity: () => number
  ) {}
  validate({ size }: { size: number }) {
    if (this.getTopicCapacity() + size > this.topicMaxCapacity) {
      throw new Error(`Exceeds topic max size ${this.topicMaxCapacity}`);
    }
  }
}
interface MetadataInput
  extends Pick<
    MessageMetadata,
    "priority" | "correlationId" | "ttd" | "ttl" | "dedupId"
  > {}
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
    return Promise.all(
      batch.map(async (data) => {
        const meta = new MessageMetadata();
        Object.assign(meta, metadataInput);
        meta.id = uniqueIntGenerator();

        try {
          const message = await this.codec.encode(data);
          this.validators.forEach((v) =>
            v.validate({ data, size: message.length, dedupId: meta.dedupId })
          );
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
// SRC/MESSAGE_STORAGE_SERVICE.TS
// wal: message durability, write as fast as possible: setImmediate flush + no snappy
interface IWriteAheadLog {
  append(data: Buffer): Promise<number | void>;
  read(offset: number, length: number): Promise<Buffer | void>;
  truncate(upToOffset: number): Promise<void>;
  close(): Promise<void>;
  getMetrics(): Promise<{
    fileSize: number | undefined;
    batchSize: number;
    batchCount: number;
    isFlushing: boolean;
  }>;
}
class WriteAheadLog implements IWriteAheadLog {
  private fileHandle?: fs.FileHandle;
  private flushPromise?: Promise<void>;
  private isFlushing = false;
  private batch: Buffer[] = [];
  private batchSize = 0;

  constructor(
    private filePath: string,
    private maxBatchSizeBytes = 1 * 1024 * 1024,
    private logger?: ILogCollector
  ) {
    this.init();
  }

  private async init() {
    try {
      await fs.mkdir(path.dirname(this.filePath), { recursive: true });
      this.fileHandle = await fs.open(this.filePath, "a+");
    } catch (error) {
      this.logger?.log("Failed to initialize WAL", { error }, "error");
    }
  }

  private scheduleFlush(): Promise<void> | void {
    if (!this.fileHandle) return;
    if (this.isFlushing) return this.flushPromise!;
    this.isFlushing = true;

    this.flushPromise = new Promise<void>(async (resolve, reject) => {
      setImmediate(async () => {
        if (!this.fileHandle) return;
        try {
          const toWrite = Buffer.concat(this.batch);
          await this.fileHandle.write(toWrite);
          await this.fileHandle.sync();
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

  // api

  async append(data: Buffer): Promise<number | void> {
    if (!this.fileHandle) return;

    try {
      const { size: offset } = await this.fileHandle.stat();
      this.batch.push(data);
      this.batchSize += data.length;

      if (this.batchSize > this.maxBatchSizeBytes) {
        this.scheduleFlush();
      }

      return offset;
    } catch (error) {
      this.logger?.log("Failed to append to WAL", { error }, "error");
    }
  }

  async read(offset: number, length: number): Promise<Buffer | void> {
    if (!this.fileHandle) return;

    try {
      const buffer = Buffer.alloc(length);
      await this.fileHandle.read(buffer, 0, length, offset);
      return buffer;
    } catch (error) {
      this.logger?.log("Failed to read from WAL", { error }, "error");
    }
  }

  async truncate(upToOffset: number): Promise<void> {
    try {
      if (!this.fileHandle) {
        this.fileHandle = await fs.open(this.filePath, "r+");
      }

      await this.fileHandle.truncate(upToOffset);
      await this.fileHandle.sync();
    } catch (error) {
      this.logger?.log("Failed to truncate the WAL", { error }, "error");
    }
  }

  async close() {
    if (!this.fileHandle) return;
    if (this.batch.length > 0) {
      await this.scheduleFlush();
    }
    await this.fileHandle.close();
  }

  async getMetrics() {
    const stats = await this.fileHandle?.stat();
    return {
      fileSize: stats?.size,
      batchSize: this.batchSize,
      batchCount: this.batch.length,
      isFlushing: this.isFlushing,
    };
  }
}
// message_log: snappy, +fast_read(scan_within_segments), +easy_retention(delete_whole_segment), +compactable(create_new_segments_with_only_actual_data)
interface ISegmentInfo {
  id: number;
  filePath: string;
  indexFilePath: string;
  baseOffset: number;
  lastOffset: number;
  size: number;
  messageCount: number;
  fileHandle?: fs.FileHandle;
}
interface ISegmentRecord {
  segmentId: number;
  offset: number;
  length: number;
  messageOffset: number;
}
interface ISegmentManager {
  getMaxSegmentSizeBytes(): number;
  close(): Promise<void>;
  getSegments(): Map<number, ISegmentInfo>;
  getCurrentSegment(): ISegmentInfo | undefined;
  setCurrentSegment(segment: ISegmentInfo): void;
  getAllSegments(): ISegmentInfo[];
}
class SegmentManager implements ISegmentManager {
  static HEADER_SIZE = 24;
  private segments = new Map<number, ISegmentInfo>();
  private currentSegment?: ISegmentInfo;

  constructor(
    private baseDir: string,
    private maxSegmentSizeBytes: number,
    private logger?: ILogCollector
  ) {
    this.init();
  }

  private async init(): Promise<void> {
    await this.loadExistingSegments();
    await this.ensureCurrentSegment();
  }

  private async loadExistingSegments(): Promise<void> {
    try {
      const files = await fs.readdir(this.baseDir);
      for (const file of files.sort()) {
        if (!file.endsWith(".segment")) continue;
        const id = parseInt(file.split(".")[0], 10);
        const filePath = path.join(this.baseDir, file);
        const indexFilePath = filePath.replace(".segment", ".index");

        const fileHandle = await fs.open(filePath, "r");
        const header = Buffer.alloc(SegmentManager.HEADER_SIZE);
        const { bytesRead } = await fileHandle.read(
          header,
          0,
          SegmentManager.HEADER_SIZE,
          0
        );

        let baseOffset = 0;
        if (bytesRead >= SegmentManager.HEADER_SIZE) {
          baseOffset = Number(header.readBigUInt64BE(6));
        }

        const messageCount = await this.countMessagesInSegment(fileHandle);
        const stat = await fs.stat(filePath);

        this.segments.set(id, {
          id,
          filePath,
          indexFilePath,
          baseOffset,
          lastOffset: baseOffset + messageCount - 1,
          size: stat.size,
          messageCount,
          fileHandle,
        });
      }
    } catch (error) {
      this.logger?.log(
        "Failed to load MessageLog segments",
        { error },
        "error"
      );
    }
  }

  private async countMessagesInSegment(
    fileHandle: fs.FileHandle
  ): Promise<number> {
    let pos = SegmentManager.HEADER_SIZE;
    let count = 0;

    while (true) {
      const lenBuf = Buffer.alloc(8);
      const { bytesRead } = await fileHandle.read(lenBuf, 0, 8, pos);
      if (bytesRead < 8) break;

      const length = lenBuf.readUInt32BE(0);
      const checksum = lenBuf.readUInt32BE(4);
      const msgBuffer = Buffer.alloc(length);

      await fileHandle.read(msgBuffer, 0, length, pos + 8);

      if (checksum !== crc.buf(msgBuffer)) break;
      pos += 8 + length;
      count++;
    }

    return count;
  }

  private async ensureCurrentSegment(): Promise<void> {
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
    const indexFilePath = filePath.replace(".segment", ".index");
    const fileHandle = await fs.open(filePath, "w+");

    const header = Buffer.alloc(SegmentManager.HEADER_SIZE);
    header.writeUInt32BE(0xcafebabe, 0); // magic
    header.writeUInt16BE(1, 4); // version
    header.writeBigUInt64BE(BigInt(newId), 6); // base offset placeholder
    header.writeBigUInt64BE(BigInt(Date.now()), 14); // timestamp

    await fileHandle.write(header, 0, SegmentManager.HEADER_SIZE, 0);

    const indexFileHandle = await fs.open(indexFilePath, "w");
    await indexFileHandle.close();

    const segment: ISegmentInfo = {
      id: newId,
      filePath,
      indexFilePath,
      baseOffset: newId,
      lastOffset: newId,
      size: SegmentManager.HEADER_SIZE,
      messageCount: 0,
      fileHandle,
    };

    this.currentSegment = segment;
    this.segments.set(newId, segment);
  }

  getMaxSegmentSizeBytes() {
    return this.maxSegmentSizeBytes;
  }

  async close() {
    for (const seg of this.segments.values()) {
      await seg.fileHandle?.close();
    }
    if (this.currentSegment?.fileHandle) {
      await this.currentSegment.fileHandle.close();
    }
  }

  getSegments() {
    return this.segments;
  }

  getCurrentSegment() {
    return this.currentSegment;
  }

  setCurrentSegment(segment: ISegmentInfo) {
    this.currentSegment = segment;
    this.segments.set(segment.id, segment);
  }

  getAllSegments() {
    return Array.from(this.segments.values());
  }
}
interface IIndexManager {
  writeIndexEntry(segment: ISegmentInfo, record: ISegmentRecord): Promise<void>;
}
class IndexManager implements IIndexManager {
  static INDEX_ENTRY_SIZE = 12;
  constructor(private logger?: ILogCollector) {}

  async writeIndexEntry(
    segment: ISegmentInfo,
    record: ISegmentRecord
  ): Promise<void> {
    try {
      const indexEntry = Buffer.alloc(IndexManager.INDEX_ENTRY_SIZE);
      indexEntry.writeBigUInt64BE(BigInt(record.messageOffset), 0);
      indexEntry.writeUInt32BE(record.offset, 8);

      const indexHandle = await fs.open(segment.indexFilePath, "a");
      await indexHandle.write(
        indexEntry,
        0,
        IndexManager.INDEX_ENTRY_SIZE,
        segment.messageCount * IndexManager.INDEX_ENTRY_SIZE
      );
      await indexHandle.close();
    } catch (error) {
      this.logger?.log("Failed to write index entry", { error }, "error");
    }
  }
}
interface IMessageAppender {
  append(data: Buffer): Promise<ISegmentRecord | void>;
}
class MessageAppender implements IMessageAppender {
  private mutex = new Mutex();

  constructor(
    private segmentManager: ISegmentManager,
    private indexManager: IIndexManager,
    private logger?: ILogCollector
  ) {}

  async append(data: Buffer): Promise<ISegmentRecord | void> {
    const segment = this.segmentManager.getCurrentSegment();
    if (!segment) return;

    await this.mutex.acquire();
    try {
      const compressed = await snappy.compress(data);
      const checksum = crc.buf(compressed);
      const lengthBuffer = Buffer.alloc(8);
      lengthBuffer.writeUInt32BE(compressed.length, 0);
      lengthBuffer.writeUInt32BE(checksum, 4);

      const offset = segment.size;
      await segment.fileHandle!.write(lengthBuffer, 0, 8, offset);
      await segment.fileHandle!.write(
        compressed,
        0,
        compressed.length,
        offset + 8
      );

      const pointer: ISegmentRecord = {
        segmentId: segment.id,
        offset,
        length: compressed.length,
        messageOffset: segment.lastOffset + 1,
      };

      await this.indexManager.writeIndexEntry(segment, pointer);

      segment.size += 8 + compressed.length;
      segment.lastOffset += 1;
      segment.messageCount += 1;

      if (segment.size >= this.segmentManager.getMaxSegmentSizeBytes()) {
        await segment.fileHandle!.sync();
        await segment.fileHandle!.close();
        segment.fileHandle = undefined;
        this.segmentManager.setCurrentSegment({ ...segment }); // rotate
      }

      return pointer;
    } catch (error) {
      this.logger?.log("Failed to append to MessageLog", { error }, "error");
    } finally {
      this.mutex.release();
    }
  }
}
interface ILogMessageReader {
  read(pointer: ISegmentRecord): Promise<Buffer | void>;
}
class LogMessageReader implements ILogMessageReader {
  constructor(
    private segmentManager: ISegmentManager,
    private logger?: ILogCollector
  ) {}

  async read(pointer: ISegmentRecord): Promise<Buffer | void> {
    const segment = this.segmentManager.getSegments().get(pointer.segmentId);
    if (!segment) return;
    try {
      const buffer = Buffer.alloc(pointer.length);
      await segment.fileHandle!.read(
        buffer,
        0,
        pointer.length,
        pointer.offset + 8
      );
      return snappy.uncompress(buffer) as Promise<Buffer>;
    } catch (error) {
      this.logger?.log("Failed to read from MessageLog", { error }, "error");
    }
  }
}
interface ICompactor {
  compactSingleSegment(
    segmentId: number,
    offsetsToDelete: number[]
  ): Promise<void>;
}
class Compactor implements ICompactor {
  static HEADER_SIZE = 24;

  constructor(
    private baseDir: string,
    private segmentManager: ISegmentManager,
    private logger?: ILogCollector
  ) {}

  async compactSingleSegment(
    segmentId: number,
    offsetsToDelete: number[]
  ): Promise<void> {
    const segment = this.segmentManager.getSegments().get(segmentId);
    if (!segment) return;

    try {
      const allRecords = await this.readAllFromSegment(segment);
      const setToDelete = new Set(offsetsToDelete);
      const filteredRecords = allRecords.filter(
        (r) => !setToDelete.has(r.offset)
      );

      const newSegment = await this.createNewCompactedSegment(
        segment,
        filteredRecords
      );

      await this.replaceSegment(segment, newSegment);

      this.segmentManager.getSegments().delete(segment.id);
      this.segmentManager.getSegments().set(newSegment.id, newSegment);

      if (this.segmentManager.getCurrentSegment()?.id === segment.id) {
        this.segmentManager.setCurrentSegment(newSegment);
      }
    } catch (error) {
      this.logger?.log(
        `Failed to compact segment ${segmentId}`,
        { error },
        "error"
      );
    }
  }

  private async readAllFromSegment(
    segment: ISegmentInfo
  ): Promise<ISegmentRecord[]> {
    const result: ISegmentRecord[] = [];
    let pos = Compactor.HEADER_SIZE;

    while (true) {
      const lenBuf = Buffer.alloc(8);
      let handle = segment.fileHandle;
      if (!handle) handle = await fs.open(segment.filePath, "r");

      const { bytesRead } = await handle.read(lenBuf, 0, 8, pos);
      if (bytesRead < 8) break;

      const length = lenBuf.readUInt32BE(0);
      const checksum = lenBuf.readUInt32BE(4);
      const msgBuffer = Buffer.alloc(length);

      await handle.read(msgBuffer, 0, length, pos + 8);

      if (checksum !== crc.buf(msgBuffer)) break;

      const offsets = result.map((r) => r.messageOffset);
      const messageOffset = Math.max(...offsets, segment.baseOffset - 1) + 1;

      result.push({
        segmentId: segment.id,
        offset: pos,
        length,
        messageOffset,
      });

      pos += 8 + length;
      if (!segment.fileHandle) await handle.close();
    }
    return result;
  }

  private async createNewCompactedSegment(
    original: ISegmentInfo,
    filteredRecords: ISegmentRecord[]
  ): Promise<ISegmentInfo> {
    const newFilePath = path.join(this.baseDir, `${original.id}.compacted`);
    const newFileHandle = await fs.open(newFilePath, "w+");

    let originalHandle = original.fileHandle;
    if (!originalHandle) {
      originalHandle = await fs.open(original.filePath, "r");
    }

    const header = Buffer.alloc(Compactor.HEADER_SIZE);
    const { bytesRead } = await originalHandle.read(
      header,
      0,
      Compactor.HEADER_SIZE,
      0
    );
    if (bytesRead < Compactor.HEADER_SIZE) {
      throw new Error("Invalid segment header");
    }

    await newFileHandle.write(header, 0, Compactor.HEADER_SIZE, 0);

    let currentPos = Compactor.HEADER_SIZE;

    for (const record of filteredRecords) {
      const buffer = Buffer.alloc(record.length);
      let handle = original.fileHandle;

      if (!handle) handle = await fs.open(original.filePath, "r");
      await handle.read(buffer, 0, record.length, record.offset);
      if (!original.fileHandle) await handle.close();

      await newFileHandle.write(buffer, 0, buffer.length, currentPos);
      currentPos += record.length;
    }

    await newFileHandle.sync();
    await newFileHandle.close();

    return {
      id: original.id,
      filePath: newFilePath,
      baseOffset: original.baseOffset,
      size: currentPos,
      messageCount: filteredRecords.length,
      fileHandle: undefined,
    } as ISegmentInfo;
  }

  private async replaceSegment(
    oldSegment: ISegmentInfo,
    newSegment: ISegmentInfo
  ): Promise<void> {
    const oldPath = oldSegment.filePath;
    const newPath = newSegment.filePath;

    await fs.unlink(oldPath).catch(() => {});
    await fs.rename(newPath, oldPath);
    const fileHandle = await fs.open(oldPath, "r+");

    newSegment.fileHandle = fileHandle;
  }
}
interface IMessageLog {
  append(data: Buffer): Promise<ISegmentRecord | void>;
  read(pointer: ISegmentRecord): Promise<Buffer | void>;
  compactSegmentsByRemovingOldMessages(
    pointers: ISegmentRecord[]
  ): Promise<void>;
  close(): Promise<void>;
  getMetrics(): Promise<{
    totalSize: number;
    messageCount: number;
    currentSegmentId: number | undefined;
    segmentCount: number;
  }>;
}
class MessageLog implements IMessageLog {
  constructor(
    private segmentManager: ISegmentManager,
    private appender: IMessageAppender,
    private reader: ILogMessageReader,
    private compactor: ICompactor,
    private logger?: ILogCollector
  ) {}

  async append(data: Buffer): Promise<ISegmentRecord | void> {
    return this.appender.append(data);
  }

  async read(pointer: ISegmentRecord): Promise<Buffer | void> {
    return this.reader.read(pointer);
  }

  async compactSegmentsByRemovingOldMessages(
    pointers: ISegmentRecord[]
  ): Promise<void> {
    const recordsBySegment = new Map<number, Set<number>>();
    for (const pointer of pointers) {
      if (!recordsBySegment.has(pointer.segmentId)) {
        recordsBySegment.set(pointer.segmentId, new Set());
      }
      recordsBySegment.get(pointer.segmentId)?.add(pointer.offset);
    }

    const compactionPromises: Promise<void>[] = [];
    for (const [segmentId, offsetsToDelete] of recordsBySegment.entries()) {
      compactionPromises.push(
        this.compactor.compactSingleSegment(segmentId, [...offsetsToDelete])
      );
    }

    try {
      await Promise.all(compactionPromises);
    } catch (error) {
      this.logger?.log("Failed to compact MessageLog", { error }, "error");
    }
  }

  async close() {
    await this.segmentManager.close();
  }

  async getMetrics() {
    const segments = this.segmentManager.getAllSegments();
    const currentSegmentId = this.segmentManager.getCurrentSegment()?.id;
    const [totalSize, messageCount] = segments.reduce(
      (sum, seg) => {
        sum[0] += seg.size;
        sum[1] += seg.messageCount;
        return sum;
      },
      [0, 0]
    );

    return {
      totalSize,
      messageCount,
      currentSegmentId,
      segmentCount: segments.length,
    };
  }
}
// message_store
interface IWALReplayer {
  replay(): Promise<void>;
}
class WALReplayer implements IWALReplayer {
  constructor(
    private wal: IWriteAheadLog,
    private log: IMessageLog,
    private db: Level<string, Buffer>,
    private codec: ICodec,
    private logger?: ILogCollector
  ) {}

  async replay(): Promise<void> {
    let offset = 0;

    try {
      while (true) {
        const offsetBuffer = await this.db.get("last_wal_offset");
        if (offsetBuffer) offset = +offsetBuffer.toString();

        this.logger?.log(`Replaying WAL from offset ${offset}`, { offset });

        // 1. Read total length
        const totalLengthBytes = await this.wal.read(offset, 4);
        if (!totalLengthBytes || totalLengthBytes.length < 4) break;
        const totalLength = totalLengthBytes.readUInt32BE(0);

        // 2. Read full record body (metaLength + metadata + message)
        const recordBytes = await this.wal.read(offset + 4, totalLength);
        if (!recordBytes || recordBytes.length < totalLength) break;

        // 3. Extract metadata length
        const metaLength = recordBytes.readUInt32BE(0);
        const metaBuffer = recordBytes.slice(4, 4 + metaLength);
        const messageBuffer = recordBytes.slice(4 + metaLength);

        // 4. Decode and apply
        const meta = await this.codec.decodeMetadata(metaBuffer);
        const pointer = await this.log.append(messageBuffer);
        if (!pointer) break;

        const pointerBuffer = await this.codec.encodePointer(pointer);
        await this.db.batch([
          { type: "put", key: `meta!${meta.id}`, value: metaBuffer },
          { type: "put", key: `ptr!${meta.id}`, value: pointerBuffer },
        ]);

        // 5. Update WAL progress
        offset = offset + 4 + totalLength;
        await this.db.put("last_wal_offset", Buffer.from(String(offset)));

        // 6. TODO: go to publishingService
      }

      if (offset > 0) {
        await this.wal.truncate(offset);
        this.logger?.log(`Replayed from WAL`, { offset });
      }
    } catch (error) {
      this.logger?.log("Failed WAL replay", { error }, "error");
    }
  }
}
interface IMessageWriter {
  write(message: Buffer, meta: MessageMetadata): Promise<number | undefined>;
}
class MessageWriter implements IMessageWriter {
  constructor(
    private wal: IWriteAheadLog,
    private log: IMessageLog,
    private db: Level<string, Buffer>,
    private codec: ICodec,
    private logger?: ILogCollector,
    private maxMessageTTLMs = 3_600_000_000
  ) {}

  async write(
    message: Buffer,
    meta: MessageMetadata
  ): Promise<number | undefined> {
    try {
      const metaBuffer = await this.codec.encodeMetadata(meta);

      // Add length prefixes
      const metaLengthBuffer = Buffer.alloc(4);
      metaLengthBuffer.writeUInt32BE(metaBuffer.length, 0);
      const totalLength = 4 + metaBuffer.length + message.length;
      const totalLengthBuffer = Buffer.alloc(4);
      totalLengthBuffer.writeUInt32BE(totalLength, 0);

      // Combine into one wal record
      const walRecord = Buffer.concat([
        totalLengthBuffer,
        metaLengthBuffer,
        metaBuffer,
        message,
      ]);

      // 1. Write to WAL first for durability
      const walOffset = await this.wal.append(walRecord);
      if (!walOffset) throw new Error("Failed writing to WAL");

      // 2. Write to log for long-term storage
      const pointer = await this.log.append(message);
      if (!pointer) throw new Error("Failed writing to MessageLog");

      // 3. Store metadata, pointers and ttl in db.
      // Why pointers are not part of metadata: metadata is often changed, with smaller values Level is faster
      // TODO: this.codec.encodePointer(pointer)
      const pointerBuffer = await this.codec.encodePointer(pointer);
      const ttl = meta.ts + (meta.ttl || this.maxMessageTTLMs);
      await this.db.batch([
        { type: "put", key: `meta!${meta.id}`, value: metaBuffer },
        { type: "put", key: `ptr!${meta.id}`, value: pointerBuffer },
        {
          type: "put",
          key: `ttl!${ttl}:${meta.id}`,
          value: 1,
        },
      ]);

      // Update last_wal_offset immediately
      const lastWalOffset = String(walOffset + 4 + totalLength);
      await this.db.put("last_wal_offset", Buffer.from(lastWalOffset));

      return meta.id;
    } catch (error) {
      this.logger?.log("Failed to write message", { ...meta, error }, "error");
      return undefined;
    }
  }
}
interface IMessageReader<Data> {
  read(
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
}
class MessageReader<Data> implements IMessageReader<Data> {
  constructor(
    private log: IMessageLog,
    private db: Level<string, Buffer>,
    private codec: ICodec,
    private logger?: ILogCollector
  ) {}

  async read(id: number) {
    return Promise.all([this.readMessage(id), this.readMetadata(id)]);
  }

  async readMessage(id: number) {
    try {
      const pointerBuffer = await this.db.get(`ptr!${id}`);
      if (!pointerBuffer) return;
      const pointer =
        await this.codec.decodePointer<ISegmentRecord>(pointerBuffer);
      const messageBuffer = await this.log.read(pointer);
      if (!messageBuffer) return;
      return this.codec.decode<Data>(messageBuffer);
    } catch (error) {
      this.logger?.log("Failed message reading", { id, error }, "error");
    }
  }

  async readMetadata<K extends keyof MessageMetadata>(
    id: number,
    keys?: K[]
  ): Promise<Pick<MessageMetadata, K> | undefined> {
    try {
      const metaBuffer = await this.db.get(`meta!${id}`);
      if (metaBuffer) return;
      return this.codec.decodeMetadata(metaBuffer, keys);
    } catch (error) {
      this.logger?.log("Failed metadata reading", { id, error }, "error");
    }
  }
}
interface IMessageRetentionManager {
  start(): void;
  stop(): void;
  markDeletable(id: number): Promise<void>;
  unmarkDeletable(id: number): Promise<void>;
}
class MessageRetentionManager implements IMessageRetentionManager {
  private retentionTimer?: NodeJS.Timeout;

  constructor(
    private db: Level<string, Buffer>,
    private wal: IWriteAheadLog,
    private log: IMessageLog,
    private dlqManager: IDLQManager<any>,
    private codec: ICodec,
    private logger?: ILogCollector,
    private retentionMs = 3_600_000
  ) {}

  start(): void {
    if (this.retentionMs === Infinity) return;
    this.retentionTimer = setInterval(
      this.retain,
      Math.min(this.retentionMs, 3_600_000)
    );
  }

  stop() {
    clearInterval(this.retentionTimer);
  }

  async markDeletable(id: number): Promise<void> {
    return this.db.put(`del!${id}`, Buffer.from("1"));
  }

  async unmarkDeletable(id: number): Promise<void> {
    return this.db.del(`del!${id}`);
  }

  private async clearDeletable() {
    const pointersToDelete: ISegmentRecord[] = [];
    const batch = this.db.batch();

    for await (const [key] of this.db.iterator({
      gt: `del!`,
      lt: `del~`,
    })) {
      const id = key.slice(4);

      try {
        const pointerBuffer = await this.db.get(`ptr!${id}`);
        if (pointerBuffer) {
          const pointer = await this.codec.decodePointer(pointerBuffer);
          pointersToDelete.push(pointer);
        }

        batch.del(`meta!${id}`);
        batch.del(`ptr!${id}`);
        batch.del(`del!${id}`);
      } catch (error) {
        this.logger?.log(
          `MessageStore cannot delete ${id}`,
          { error, id },
          "error"
        );
      }
    }

    // 1. db batch
    await batch.write();

    // 2. delete from log
    if (pointersToDelete.length) {
      await this.log.compactSegmentsByRemovingOldMessages(pointersToDelete);
    }

    // 3. truncate the wal
    const offsetBuffer = await this.db.get("last_wal_offset");
    await this.wal.truncate(+offsetBuffer.toSorted());

    return pointersToDelete.length;
  }

  private async processTtl() {
    for await (const [key] of this.db.iterator({
      lt: `ts!${Date.now() + this.retentionMs}`,
    })) {
      const id = key.split(":")[1];

      try {
        const metaBuffer = await this.db.get(`meta!${id}`);
        const meta = await this.codec.decodeMetadata(metaBuffer);

        await this.db.del(key);
        this.dlqManager.enqueue(meta, "expired");
      } catch (error) {
        this.logger?.log(
          `MessageStore cannot send to dlq ${id}`,
          { error, id },
          "error"
        );
      }
    }
  }

  private retain = async () => {
    try {
      const [deletedCount] = await Promise.all([
        this.clearDeletable(),
        this.processTtl(),
      ]);

      this.logger?.log("MessageStore retention succeed", { deletedCount });
    } catch (error) {
      this.logger?.log("MessageStore retention failed", { error }, "error");
    }
  };
}
interface IMessageStore<Data> {
  write(message: Buffer, meta: MessageMetadata): Promise<number | undefined>;
  read(
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
  markDeletable(id: number): Promise<void>;
  unmarkDeletable(id: number): Promise<void>;
  close(): Promise<void>;
  getMetrics(): Promise<{
    wal: {
      fileSize: number | undefined;
      batchSize: number;
      batchCount: number;
      isFlushing: boolean;
    };
    log: {
      totalSize: number;
      messageCount: number;
      currentSegmentId: number | undefined;
      segmentCount: number;
    };
    db: {};
    ram: NodeJS.MemoryUsage;
  }>;
}
class MessageStoreService<Data> implements IMessageStore<Data> {
  constructor(
    private replayer: IWALReplayer,
    private writer: IMessageWriter,
    private reader: IMessageReader<Data>,
    private retentionManager: IMessageRetentionManager,
    private db: Level<string, Buffer>,
    private wal: IWriteAheadLog,
    private log: IMessageLog
  ) {
    this.init();
  }

  private async init() {
    await this.replayer.replay();
    this.retentionManager.start();
  }

  async write(
    message: Buffer,
    meta: MessageMetadata
  ): Promise<number | undefined> {
    return this.writer.write(message, meta);
  }

  async read(id: number) {
    return this.reader.read(id);
  }

  async readMessage(id: number): Promise<Data | undefined> {
    return this.reader.readMessage(id);
  }

  async readMetadata<K extends keyof MessageMetadata>(
    id: number,
    keys?: K[]
  ): Promise<Pick<MessageMetadata, K> | undefined> {
    return this.reader.readMetadata(id, keys);
  }

  async markDeletable(id: number): Promise<void> {
    return this.retentionManager.markDeletable(id);
  }

  async unmarkDeletable(id: number): Promise<void> {
    return this.retentionManager.markDeletable(id);
  }

  async close() {
    this.retentionManager.stop();
    await Promise.all([this.wal.close(), this.db.close()]);
  }

  async getMetrics() {
    const [walStats, logStats] = await Promise.all([
      this.wal.getMetrics(),
      this.log.getMetrics(),
    ]);

    return {
      wal: walStats,
      log: logStats,
      db: {
        /** db.stats() is not implemented in Level */
      },
      ram: process.memoryUsage(),
    };
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
    private deliveryTracker: IDeliveryTracker,
    private dlq: IDLQManager<Data>
  ) {}
  process(meta: MessageMetadata): boolean {
    const attempts = this.deliveryTracker.decrementDeliveryAttempts(meta.id);
    const shouldDeadLetter = attempts === 0;
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
    dlqManager: IDLQManager<Data>,
    deliveryTracker: IDeliveryTracker,
    delayedMessageManager: DelayedMessageManager<Data>
  ): IMessagePipeline;
}
class PipelineFactory<Data> implements IPipelineFactory<Data> {
  create(
    dlqManager: IDLQManager<Data>,
    deliveryTracker: IDeliveryTracker,
    delayedMessageManager: IDelayedMessageManager
  ) {
    const pipeline = new MessagePipeline();
    pipeline.addProcessor(new ExpirationProcessor(dlqManager));
    pipeline.addProcessor(new DelayProcessor(delayedMessageManager));
    pipeline.addProcessor(new AttemptsProcessor(deliveryTracker, dlqManager));

    return pipeline;
  }
}
interface IDeduplicationTracker {
  has(id: string): boolean;
  add(id: string): void;
}
class DeduplicationTracker implements IDeduplicationTracker {
  private seen: IPersistedMap<string, number>; // dedupId:timestamp
  constructor(
    mapFactory: IPersistedMapFactory,
    private deduplicationWindowMs = 300_000
  ) {
    this.seen = mapFactory.create<string, number>("seen");
  }

  has(id: string): boolean {
    const ts = this.seen.get(id);
    if (!ts) return false;
    if (Date.now() - ts > this.deduplicationWindowMs) {
      this.seen.delete(id);
      return false;
    }
    return true;
  }

  add(id: string): void {
    this.seen.set(id, Date.now());
  }
}
interface IDelayMonitor<Data> {
  setReadyCallback(onReadyHandler: (data: Data) => Promise<void>): void;
  schedule(data: Data, readyTs: number): void;
  pendingsCount(): number;
  cleanup(): void;
}
class DelayMonitor<Data> implements IDelayMonitor<Data> {
  private onReadyCallback?: (data: Data) => Promise<void>;
  private nextTimeout?: NodeJS.Timeout;
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
        this.onReadyCallback?.(data);
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
  enqueue(meta: MessageMetadata, consumerId?: number): void;
  getMetrics(): {
    count: number;
  };
}
class DelayedMessageManager<Data> implements IDelayedMessageManager {
  constructor(
    private delayMonitor: IDelayMonitor<[number, number | undefined]>,
    private messageStore: IMessageStore<Data>,
    private messageRouter: IMessageRouter,
    private deliveryTracker: IDeliveryTracker,
    private queueManager: IQueueManager,
    private logger?: ILogCollector
  ) {
    delayMonitor.setReadyCallback(this.dequeue);
  }

  enqueue(meta: MessageMetadata, consumerId?: number) {
    if (!meta.ttd) return;
    const delay = meta.ts + meta.ttd;
    this.delayMonitor.schedule([meta.id, consumerId], delay);
    this.logger?.log(`Message is delayed until ${delay}.`, meta);
  }

  private dequeue = async (data: [number, number | undefined]) => {
    const [messageId, consumerId] = data;
    const meta = await this.messageStore.readMetadata(messageId);
    if (!meta) return;

    if (consumerId) {
      this.queueManager.enqueue(consumerId, meta);
      this.logger?.log(`Message is requeued to ${consumerId}.`, meta);
      return;
    }

    const deliveryCount = await this.messageRouter.route(meta);
    this.deliveryTracker.setAwaitedDeliveries(meta.id, deliveryCount);
    this.logger?.log(`Message is routed to ${meta.topic}.`, meta);
  };

  getMetrics() {
    return {
      count: this.delayMonitor.pendingsCount(),
    };
  }
}
interface IConsumerGroup {
  name: string;
  addMember(id: number, routingKeys?: string[]): void;
  removeMember(id: number): void;
  hasMembers(): boolean;
  getMembers(
    messageId: number,
    correlationId?: string
  ): Iterable<number> | undefined;
  isDefaultGroup(): boolean;
  getMemberRoutingKeys(id: number): Array<string> | undefined;
  getMetrics(): {
    name: string;
    count: number;
  };
}
class ConsumerGroup implements IConsumerGroup {
  static defaultName = "non-grouped";
  private members: IPersistedMap<number, string[]>;

  constructor(
    public name: string = ConsumerGroup.defaultName,
    mapFactory: IPersistedMapFactory,
    private hashRing: IHashRing
  ) {
    this.members = mapFactory.create<number, string[]>(`members!${name}`);
  }

  addMember(id: number, routingKeys?: string[]) {
    // for the group with defined groupId we need to enforce homogeneous routingKeys within the members
    if (this.name && this.members.size > 0) {
      const expectedKeys = this.members.values().next().value as
        | Array<string>
        | undefined;
      const isValid =
        expectedKeys?.length === routingKeys?.length &&
        routingKeys?.every((k) => expectedKeys?.includes(k));

      if (!isValid) {
        throw new Error(
          `Member ${id} has incompatible routingKeys for group ${this.name}`
        );
      }
    }

    this.hashRing.addNode(id);
    this.members.set(id, routingKeys || []);
  }

  removeMember(id: number) {
    this.hashRing.removeNode(id);
    this.members.delete(id);
  }

  hasMembers(): boolean {
    return this.hashRing.getNodeCount() > 0;
  }

  getMembers(
    messageId: number,
    correlationId?: string
  ): Iterable<number> | undefined {
    if (this.name || correlationId) {
      return this.hashRing.getNode(correlationId || messageId.toString());
    }
    return this.members.keys();
  }

  isDefaultGroup() {
    return this.name === ConsumerGroup.defaultName;
  }

  getMemberRoutingKeys(id: number) {
    return this.members.get(id);
  }

  getMetrics() {
    return {
      name: this.name,
      count: this.members.size,
    };
  }
}
class ConsumerGroupSerializer implements ISerializable {
  constructor(private mapFactory: IPersistedMapFactory) {}
  serialize(group: IConsumerGroup) {
    return group.name;
  }
  deserialize(groupId: string) {
    return new ConsumerGroup(
      groupId,
      this.mapFactory,
      new InMemoryHashRing(new SHA256HashService(), this.mapFactory, groupId)
    );
  }
}
interface IMessageRouter {
  addConsumer(id: number, groupId?: string, routingKeys?: string[]): void;
  removeConsumer(id: number): void;
  route(meta: MessageMetadata, skipDLQ?: boolean): Promise<number>;
  routeBatch(metas: MessageMetadata[]): Promise<number[]>;
  getMetrics(): {
    consumerGroups: {
      name: string;
      count: number;
    }[];
  };
}
class MessageRouter<Data> implements IMessageRouter {
  private consumerGroups: IPersistedMap<string, IConsumerGroup>;

  constructor(
    private mapFactory: IPersistedMapFactory,
    private clientManager: IClientManager,
    private queueManager: IQueueManager,
    private subscriptionManager: ISubscriptionManager<Data>,
    private dlqManager: IDLQManager<Data>,
    private processedMessageTracker: IProcessedMessageTracker
  ) {
    this.consumerGroups = mapFactory.create<string, IConsumerGroup>(
      `groups`,
      new ConsumerGroupSerializer(mapFactory)
    );
  }

  getMetrics() {
    return {
      consumerGroups: Array.from(this.consumerGroups.values()).map((group) =>
        group.getMetrics()
      ),
    };
  }

  addConsumer(
    id: number,
    groupId = ConsumerGroup.defaultName,
    routingKeys?: string[]
  ) {
    if (!this.consumerGroups.has(groupId)) {
      const hashRing = new InMemoryHashRing(
        new SHA256HashService(),
        this.mapFactory,
        groupId
      );
      this.consumerGroups.set(
        groupId,
        new ConsumerGroup(groupId, this.mapFactory, hashRing)
      );
    }

    this.consumerGroups.get(groupId)!.addMember(id, routingKeys);
  }

  removeConsumer(id: number) {
    for (const [name, group] of this.consumerGroups.entries()) {
      group.removeMember(id);
      if (!group.hasMembers()) {
        group;
        this.consumerGroups.delete(name);
      }
    }
  }

  async route(meta: MessageMetadata, skipDLQ = false): Promise<number> {
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
    if (!skipDLQ && !processedCount) {
      this.dlqManager.enqueue(meta, "no_consumers");
    }

    return deliveryCount;
  }

  async routeBatch(metas: MessageMetadata[]): Promise<number[]> {
    return Promise.all(metas.map((msg) => this.route(msg)));
  }

  private async groupRoute(group: IConsumerGroup, meta: MessageMetadata) {
    const { routingKey, correlationId, id } = meta;
    const isSingleConsumer = !group.isDefaultGroup() || correlationId;
    const candidates = group.getMembers(id, correlationId);
    if (!candidates) return;

    let fallbackCandidateId;
    let processedCount = 0;
    let deliveryCount = 0;
    const now = Date.now();

    for (const candidateId of candidates) {
      // filter candidate
      if (!this.isSuitable(group, candidateId, meta.id, now, routingKey))
        continue;

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
    messageId: number,
    now: number,
    routingKey?: string
  ) {
    // skip those who have processed it (exactly-once)
    if (this.processedMessageTracker.has(consumerId, messageId)) return false;

    // skip non-operable members (backpressure)
    if (!this.clientManager.isOperable(consumerId, now)) return false;

    // filter by keys
    const expectedKeys = group.getMemberRoutingKeys(consumerId);
    return !expectedKeys?.length || expectedKeys.indexOf(routingKey!) !== -1;
  }
}
interface IPublishingService {
  publish(
    producerId: number,
    message: Buffer,
    meta: MessageMetadata
  ): Promise<void>;
  getMetrics(): {
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
    private readonly messageStore: IMessageStore<Data>,
    private readonly pipeline: IMessagePipeline,
    private readonly messageRouter: IMessageRouter,
    private readonly deliveryTracker: IDeliveryTracker,
    private readonly clientManager: IClientManager,
    private readonly delayedManager: IDelayedMessageManager,
    private readonly metrics: IMetricsCollector,
    private readonly logger?: ILogCollector
  ) {}

  async publish(
    producerId: number,
    message: Buffer,
    meta: MessageMetadata
  ): Promise<void> {
    await this.messageStore.write(message, meta);

    const processingTime = Date.now() - meta.ts;
    this.metrics.recordEnqueue(message.length, processingTime);
    this.clientManager.recordActivity(producerId, {
      messageCount: 1,
      processingTime,
      status: "idle",
    });

    if (this.pipeline.process(meta)) return;
    const deliveryCount = await this.messageRouter.route(meta);
    this.deliveryTracker.setAwaitedDeliveries(meta.id, deliveryCount);

    this.logger?.log(`Message is routed to ${meta.topic}.`, meta);
  }

  getMetrics() {
    return {
      router: this.messageRouter.getMetrics(),
      delayedMessages: this.delayedManager.getMetrics(),
      storage: this.messageStore.getMetrics(),
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
  getMetrics(): {
    size: number;
  };
}
class QueueManager implements IQueueManager {
  private queues: IPersistedMap<number, IPersistedQueue<number>>;
  private totalQueuedMessages = 0;

  constructor(
    mapFactory: IPersistedMapFactory,
    private queueFactory: IPersistedQueueFactory
  ) {
    this.queues = mapFactory.create<number, IPersistedQueue<number>>(
      `queues`,
      new PersistedQueueSerializer(queueFactory, (n: number) => n)
    );
  }

  addQueue(id: number) {
    const queue = this.queueFactory.create(`queue!${id}`, (n: number) => n);
    this.queues.set(id, queue);
  }

  removeQueue(id: number) {
    const queue = this.queues.get(id);
    if (!queue) return;
    queue.clear();
    queue.flush().then(() => {
      this.queues.delete(id);
    });
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

  getMetrics() {
    return {
      size: this.totalQueuedMessages,
    };
  }
}
interface IConsumptionService<Data> {
  consume(consumerId: number, noAck?: boolean): Promise<Data | undefined>;
  getMetrics(): {
    queuedMessages: {
      size: number;
    };
  };
}
class ConsumptionService<Data> implements IConsumptionService<Data> {
  constructor(
    private readonly queueManager: IQueueManager,
    private readonly messageStore: IMessageStore<Data>,
    private readonly pendingAcks: IAckRegistry,
    private readonly deliveryTracker: IDeliveryTracker,
    private readonly processedMessageTracker: IProcessedMessageTracker,
    private readonly clientManager: IClientManager,
    private readonly logger?: ILogCollector
  ) {}

  async consume(consumerId: number, noAck = false): Promise<Data | undefined> {
    if (this.pendingAcks.isReachedMaxUnacked(consumerId)) return;

    const messageId = this.queueManager.dequeue(consumerId);
    if (!messageId) return;

    const [message, meta] = await this.messageStore.read(messageId);
    if (!meta || !message) return;

    if (!noAck) {
      this.pendingAcks.addAck(consumerId, messageId);
    } else {
      this.clientManager.recordActivity(consumerId, {
        messageCount: 1,
        pendingAcks: 0,
        processingTime: 0,
        status: "idle",
      });

      this.processedMessageTracker.add(consumerId, messageId);
      await this.deliveryTracker.decrementAwaitedDeliveries(messageId);
    }

    this.logger?.log(`Message is consumed from ${meta.topic}.`, meta);
    return message;
  }

  getMetrics() {
    return {
      queuedMessages: this.queueManager.getMetrics(),
    };
  }
}
//
//
//
// SRC/ACK_SERVICE.TS
interface IDeliveryTracker {
  setAwaitedDeliveries(messageid: number, deliveries: number): void;
  decrementAwaitedDeliveries(messageId: number): Promise<void>;
  decrementDeliveryAttempts(messageId: number): number;
  getDeliveryRetryBackoff(messageId: number): number;
}
interface IDeliveryEntry {
  awaited: number;
  attempts: number;
}
class DeliveryTracker<Data> implements IDeliveryTracker {
  private deliveries: IPersistedMap<number, IDeliveryEntry>;

  constructor(
    mapFactory: IPersistedMapFactory,
    private messageStore: IMessageStore<Data>,
    private metrics: IMetricsCollector,
    private readonly maxAttempts = 1,
    private readonly initialBackoffMs = 1000,
    private readonly maxBackoffMs = 30_000
  ) {
    this.deliveries = mapFactory.create<number, IDeliveryEntry>("deliveries");
  }

  private getOrCreateEntry(messageid: number) {
    const entry = this.deliveries.get(messageid);
    return entry ?? { awaited: 0, attempts: Math.max(1, this.maxAttempts) };
  }

  setAwaitedDeliveries(messageid: number, deliveries: number) {
    const entry = this.getOrCreateEntry(messageid);
    entry.awaited = deliveries;
    entry.attempts = this.maxAttempts;
    this.deliveries.set(messageid, entry);
  }

  async decrementAwaitedDeliveries(messageId: number) {
    let entry = this.deliveries.get(messageId);
    if (!entry) return;
    if (--entry.awaited > 0) {
      this.deliveries.set(messageId, entry);
      return;
    }

    this.deliveries.delete(messageId);
    // if there is no deliveries needed mark message as consumed
    await this.messageStore.markDeletable(messageId);

    const meta = await this.messageStore.readMetadata(messageId, ["ts"]);
    if (meta) this.metrics.recordDequeue(Date.now() - meta.ts);
  }

  decrementDeliveryAttempts(messageId: number): number {
    const entry = this.getOrCreateEntry(messageId);
    if (--entry.attempts == 0) {
      this.deliveries.delete(messageId);
    } else {
      this.deliveries.set(messageId, entry);
    }

    return entry.attempts;
  }

  getDeliveryRetryBackoff(messageId: number) {
    const attempts = this.deliveries.get(messageId)?.attempts;
    if (!attempts) return 0;
    return Math.min(
      this.maxBackoffMs ?? 30_000,
      this.initialBackoffMs * Math.pow(2, attempts - 1)
    );
  }
}
interface IProcessedMessageTracker {
  has(consumerId: number, messageId: number): boolean;
  add(consumerId: number, messageId: number): void;
  remove(consumerId: number, messageId: number): void;
}
class ProcessedMessageTracker implements IProcessedMessageTracker {
  private processed: IPersistedMap<number, Map<number, number>>; // consumerId:{messageId:ts}

  constructor(
    mapFactory: IPersistedMapFactory,
    private deduplicationWindowMs = 300_000
  ) {
    this.processed = mapFactory.create<number, Map<number, number>>(
      "processed",
      new MapSerializer<number, number>()
    );
  }

  has(consumerId: number, messageId: number): boolean {
    const processed = this.processed.get(consumerId);
    if (!processed) return false;
    const ts = processed.get(messageId);
    if (!ts) return false;
    if (Date.now() - ts > this.deduplicationWindowMs) {
      processed.delete(messageId);
      return false;
    }
    return true;
  }

  add(consumerId: number, messageId: number): void {
    let processed = this.processed.get(consumerId);
    if (!processed) {
      processed = new Map();
      this.processed.set(consumerId, processed);
    }
    processed.set(messageId, Date.now());
  }

  remove(consumerId: number, messageId: number): void {
    this.processed.get(consumerId)?.delete(messageId);
  }
}
interface IAckRegistry {
  addAck(consumerId: number, messageId: number): void;
  getAcks(consumerId: number): Map<number, number> | undefined;
  getAllAcks(): IPersistedMap<number, Map<number, number>>;
  removeAck(consumerId: number, messageId?: number): void;
  isReachedMaxUnacked(consumerId: number): boolean;
  getMetrics(): {
    count: number;
  };
}
class AckRegistry implements IAckRegistry {
  private acks: IPersistedMap<number, Map<number, number>>; // consumerId:{messageId:ts}

  constructor(
    mapFactory: IPersistedMapFactory,
    private clientManager: IClientManager,
    private maxUnackedPerConsumer = 10
  ) {
    this.acks = mapFactory.create<number, Map<number, number>>(
      "acks",
      new MapSerializer<number, number>()
    );
  }

  isReachedMaxUnacked(consumerId: number) {
    return this.getAcks(consumerId)?.size === this.maxUnackedPerConsumer;
  }

  getMetrics() {
    let count = 0;
    const consumerAcks = this.acks.values();
    for (const acks of consumerAcks) {
      count += acks.size;
    }
    return { count };
  }

  addAck(consumerId: number, messageId: number): void {
    if (!this.acks.has(consumerId)) {
      this.acks.set(consumerId, new Map());
    }
    this.acks.get(consumerId)?.set(messageId, Date.now());

    this.clientManager.recordActivity(consumerId, {
      pendingAcks: 1,
      status: "active",
    });
  }

  getAcks(consumerId: number) {
    return this.acks.get(consumerId);
  }

  getAllAcks() {
    return this.acks;
  }

  removeAck(consumerId: number, messageId?: number): void {
    const pendings = this.acks.get(consumerId);
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
      this.acks.delete(consumerId);
    }
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
  private onTimeoutCallback?: AckTimeoutHandler;
  private timer?: NodeJS.Timeout;

  constructor(
    private pendingAcks: IAckRegistry,
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
    const pendings = this.pendingAcks.getAllAcks();
    if (!pendings) return;
    for (const [consumerId, messages] of pendings.entries()) {
      for (const [messageId, consumedAt] of messages.entries()) {
        if (now - consumedAt > this.ackTimeoutMs) {
          await this.onTimeoutCallback?.(consumerId, messageId, false);
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
  getMetrics(): {
    pendingAcks: {
      count: number;
    };
  };
}
class AckService<Data> implements IAckService {
  constructor(
    private readonly pendingAcks: IAckRegistry,
    private readonly deliveryTracker: IDeliveryTracker,
    private readonly processedMessageTracker: IProcessedMessageTracker,
    private readonly ackMonitor: IAckMonitor,
    private readonly messageStore: IMessageStore<Data>,
    private readonly messageRouter: IMessageRouter,
    private readonly pipeline: IMessagePipeline,
    private readonly subscriptionManager: ISubscriptionManager<Data>,
    private readonly delayedMessageManager: IDelayedMessageManager,
    private readonly logger?: ILogCollector
  ) {
    this.ackMonitor.setTimeoutCallback(this.nack);
  }

  async ack(consumerId: number, messageId?: number): Promise<number[]> {
    const pendingAcks: number[] = [];

    if (messageId) {
      pendingAcks.push(messageId);
      this.pendingAcks.removeAck(consumerId, messageId);
    } else {
      const pendingMap = this.pendingAcks.getAcks(consumerId);
      if (pendingMap) pendingAcks.push(...pendingMap.keys());
      this.pendingAcks.removeAck(consumerId);
    }

    for (const messageId of pendingAcks) {
      this.processedMessageTracker.add(consumerId, messageId);
      await this.deliveryTracker.decrementAwaitedDeliveries(messageId);
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
      const meta = await this.messageStore.readMetadata(messageId);
      if (!meta) continue;
      if (this.pipeline.process(meta)) continue;

      // 1. consumer nacked with requeue=true: he will process message later so we send it to consumer queue after backoff
      // 2. consumer nacked with requeue=false: he reject to process the message so mark message as processed by consumer and
      //    reroute it with skipDLQ=true to avoid message ends up in dlq due the 'no_consumers' reason.
      //    Consumer as well as other consumers which have processed the message will not get it again.
      //    Rerouting helps to redeliver message to another consumer group member.
      // 3. AckMonitor nacked(requeue=false): same as 2

      if (requeue) {
        const delay = this.deliveryTracker.getDeliveryRetryBackoff(messageId);
        meta.ttd = Date.now() - meta.ts + delay;
        this.delayedMessageManager.enqueue(meta, consumerId);
      } else {
        this.processedMessageTracker.add(consumerId, messageId);
        const deliveryCount = await this.messageRouter.route(meta, true);
        this.deliveryTracker.setAwaitedDeliveries(meta.id, deliveryCount);
      }

      this.logger?.log(
        `Message is nacked to ${requeue ? consumerId : meta.topic}.`,
        meta,
        "warn"
      );
    }

    return messages.length;
  };

  getMetrics() {
    return {
      pendingAcks: this.pendingAcks.getMetrics(),
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
    noAck?: boolean
  ): void;
  removeListener(consumerId: number): void;
  hasListener(consumerId: number): boolean;
  pushTo(
    consumerId: number,
    meta: MessageMetadata
  ): Promise<boolean | undefined>;
  drainQueue(consumerId: number): Promise<void>;
  getMetrics(): {
    count: number;
  };
}
class SubscriptionManager<Data> implements ISubscriptionManager<Data> {
  private listeners = new Map<number, ISubscriptionListener<Data>>();
  private fanouts = new Set<number>();

  constructor(
    private clientManager: ClientManager,
    private queueManager: QueueManager,
    private messageStore: IMessageStore<Data>,
    private pendingAcks: AckRegistry,
    private deliveryTracker: DeliveryTracker<Data>,
    private processedMessageTracker: IProcessedMessageTracker,
    private logger?: ILogCollector
  ) {}

  getMetrics() {
    return {
      count: this.listeners.size,
    };
  }

  addListener(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    noAck = false
  ) {
    this.listeners.set(consumerId, listener);
    if (noAck) this.fanouts.add(consumerId);
    this.drainQueue(consumerId);
  }

  removeListener(consumerId: number) {
    this.listeners.delete(consumerId);
  }

  hasListener(consumerId: number) {
    return this.listeners.has(consumerId);
  }

  async pushTo(consumerId: number, meta: MessageMetadata) {
    const listener = this.listeners.get(consumerId);
    if (!listener) return;

    if (this.pendingAcks.isReachedMaxUnacked(consumerId)) return;

    const message = await this.messageStore.readMessage(meta.id);
    if (!message) return;

    listener(message);

    if (!this.fanouts.has(consumerId)) {
      this.pendingAcks.addAck(consumerId, meta.id);
      return true;
    }

    this.processedMessageTracker.add(consumerId, meta.id);

    setImmediate(() => {
      this.logger?.log(`Message is consumed from ${meta.topic}.`, meta);
    });

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

    const meta = await this.messageStore.readMetadata(messageId);
    if (!meta) return;

    const needAck = await this.pushTo(consumerId, meta);

    if (!needAck) {
      await this.deliveryTracker.decrementAwaitedDeliveries(messageId);
    }

    // schedule the next drain
    setImmediate(() => this.drainQueue(consumerId));
  }
}
interface ISubscriptionService<Data> {
  subscribe(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    noAck?: boolean
  ): void;
  unsubscribe(consumerId: number): void;
  getMetrics(): {
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
    noAck?: boolean
  ): void {
    this.subscriptionManager.addListener(consumerId, listener, noAck);

    this.logger?.log(`${consumerId} is subscribed to ${this.topicName}.`, {
      consumerId,
      noAck,
    });
  }

  unsubscribe(consumerId: number): void {
    this.subscriptionManager.removeListener(consumerId);

    this.logger?.log(`${consumerId} is unsubscribed from ${this.topicName}.`, {
      consumerId,
    });
  }

  getMetrics() {
    return this.subscriptionManager.getMetrics();
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
  getMetrics(): {
    size: number;
  };
}
class DLQManager<Data> implements IDLQManager<Data> {
  private messages: PersistedMap<number, DLQReason>;

  constructor(
    private topic: string,
    mapFactory: IPersistedMapFactory,
    private messageStore: IMessageStore<any>,
    private logger?: ILogCollector
  ) {
    this.messages = mapFactory.create("dlq");
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
      const [message, meta] = await this.messageStore.read(messageId);
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

  getMetrics() {
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
  getMetrics(): {
    size: number;
  };
}
class DLQService<Data> implements IDLQService<Data> {
  constructor(
    private readonly dlqManager: IDLQManager<Data>,
    private readonly clientManager: IClientManager
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

  getMetrics() {
    return this.dlqManager.getMetrics();
  }
}
//
//
//
// SRC/CLIENT_MANAGEMENT_SERVICE.TS
type ClientType = "producer" | "consumer" | "dlq_consumer";
type ClientStatus = "active" | "idle" | "lagging";
interface IClientState {
  id: number;
  clientType: ClientType;
  registeredAt: number;
  lastActiveAt: number;
  // Metrics
  status: ClientStatus;
  messageCount: number;
  processingTime: number;
  avgProcessingTime: number;
  pendingAcks: number;
}
class ClientState implements IClientState {
  public registeredAt = Date.now();
  public lastActiveAt = Date.now();
  public status: ClientStatus = "active";
  public messageCount = 0;
  public processingTime = 0;
  public avgProcessingTime = 0;
  public pendingAcks = 0;
  constructor(
    public id: number,
    public clientType: ClientType
  ) {}
}
class ClientStateSerializer {
  serialize(state: IClientState): Partial<IClientState> {
    const { lastActiveAt, ...rest } = state;
    return rest;
  }
  deserialize(state: Omit<IClientState, "lastActiveAt">): IClientState {
    const client = new ClientState(state.id, state.clientType);
    return Object.assign(client, state);
  }
}
interface IClientManager {
  addClient(type: ClientType, id: number): IClientState | undefined;
  removeClient(id: number): number;
  getClients(filter?: (client: IClientState) => boolean): Set<IClientState>;
  getClient(id: number): IClientState | undefined;
  throwIfExists(id: number): void;
  validateClient(id: number, expectedType?: ClientType): void;
  isOperable(id: number, now: number): boolean;
  isIdle(id: number): boolean;
  recordActivity(id: number, activityRecord: Partial<IClientState>): void;
  getMetrics(): {
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
  private clients: IPersistedMap<number, IClientState>;

  constructor(
    mapFactory: IPersistedMapFactory,
    private inactivityThresholdMs = 300_000,
    private processingTimeThresholdMs = 50_000,
    private pendingThresholdMs = 100
  ) {
    this.clients = mapFactory.create<number, IClientState>(
      "clients",
      new ClientStateSerializer()
    );
  }

  addClient(type: ClientType, id: number) {
    const client = this.getClient(id);

    if (client) {
      this.recordActivity(id, {
        status: "active",
      });

      return;
    }

    const state = new ClientState(id, type);
    this.clients.set(id, state);
    return state;
  }

  removeClient(id: number) {
    const client = this.clients.get(id);
    if (client) this.clients.delete(id);
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

  getClient(id: number) {
    return this.clients.get(id);
  }

  throwIfExists(id: number) {
    if (this.clients.has(id)) {
      throw new Error(`Client with ID ${id} already exists`);
    }
  }

  validateClient(id: number, expectedType?: ClientType) {
    const metadata = this.clients.get(id);
    if (!metadata) {
      throw new Error(`Client with ID ${id} not found`);
    }
    if (expectedType && metadata.clientType !== expectedType) {
      throw new Error(`Client with ID ${id} is not a ${expectedType}`);
    }
  }

  isOperable(id: number, now: number) {
    const client = this.getClient(id);
    if (!client) return false;
    if (client.status == "lagging") return false;
    if (client.avgProcessingTime > this.processingTimeThresholdMs) {
      return false;
    }
    if (client.pendingAcks > this.pendingThresholdMs) return false;
    return now - client.lastActiveAt < this.inactivityThresholdMs;
  }

  isIdle(id: number) {
    const client = this.getClient(id);
    if (!client) return false;
    return client.status === "idle";
  }

  recordActivity(id: number, activityRecord: Partial<IClientState>) {
    if (!this.clients.has(id)) return;
    const client = this.clients.get(id)!;
    client.lastActiveAt = Date.now();

    for (const k in activityRecord) {
      const key = k as keyof IClientState;
      const value = activityRecord[key];

      if (typeof client[key] === "number" && typeof value === "number") {
        (client[key] as number) = client[key] + value;
      } else if (typeof client[key] === "string" && typeof value === "string") {
        (client[key] as string) = value;
      }
    }

    if (client.messageCount > 0) {
      client.avgProcessingTime = client.processingTime / client.messageCount;
    }

    this.clients.set(id, client);
  }

  getMetrics() {
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
  id: number;
  publish(batch: Data[], metadata?: MetadataInput): Promise<IPublishResult[]>;
}
class Producer<Data> implements IProducer<Data> {
  constructor(
    private readonly publishingService: IPublishingService,
    private readonly messageFactory: IMessageFactory<Data>,
    private readonly topicName: string,
    public readonly id: number
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
        await this.publishingService.publish(this.id, message!, meta);
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
      validators.push(new SchemaValidator<Data>(schemaRegistry, schema));
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
  noAck?: boolean;
}
interface IConsumer<Data> {
  id: number;
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
    public readonly id: number,
    private readonly noAck = false,
    limit?: number
  ) {
    this.limit = Math.max(1, limit!);
  }

  async consume() {
    const messages: Data[] = [];

    for (let i = 0; i < this.limit; i++) {
      const message = await this.consumptionService.consume(
        this.id,
        this.noAck
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
    this.subscriptionService.subscribe(this.id, listener, this.noAck);
  }

  unsubscribe(): void {
    this.subscriptionService.unsubscribe(this.id);
  }
}
interface IDLQConsumer<Data> {
  id: number;
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
    public readonly id: number,
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
}
interface IClientManagementService<Data> {
  createProducer(): IProducer<Data>;
  createConsumer(config?: IConsumerConfig, id?: number): IConsumer<Data>;
  createDLQConsumer(limit?: number): DLQConsumer<Data>;
  deleteClient(id: number): void;
  getMetrics(): {
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
    config: IConsumerConfig = {},
    id = uniqueIntGenerator()
  ): IConsumer<Data> {
    this.clientManager.throwIfExists(id);
    const { groupId, routingKeys, noAck, limit } = config;
    this.messageRouter.addConsumer(id, groupId, routingKeys);
    this.clientManager.addClient("consumer", id);
    this.queueManager.addQueue(id);

    this.logger?.log(`consumer_created`, { id });

    return new Consumer(
      this.consumptionService,
      this.ackService,
      this.subscriptionService,
      id,
      noAck,
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

  getMetrics() {
    return this.clientManager.getMetrics();
  }
}
//
//
//
// SRC/TOPIC.TS
interface ITopicConfig {
  schema?: string; // registered schema` name
  persistThresholdMs?: number; // flush delay, 1000 default, if need ephemeral set Infinity
  retentionMs?: number; // 86_400_000 1 day
  maxSizeBytes?: number;
  maxDeliveryAttempts?: number;
  maxMessageSize?: number;
  maxMessageTTLMs?: number;
  ackTimeoutMs?: number; // e.g., 30_000
  consumerInactivityThresholdMs?: number; // 600_000;
  consumerProcessingTimeThresholdMs?: number;
  consumerPendingThresholdMs?: number;
  initialBackoffMs?: number; // e.g., 1000 ms
  maxBackoffMs?: number; // e.g., 30_000 ms
  deduplicationWindowMs?: number;
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
  getMetrics(): Promise<{
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
    storage: {
      wal: {
        fileSize: number | undefined;
        batchSize: number;
        batchCount: number;
        isFlushing: boolean;
      };
      log: {
        totalSize: number;
        messageCount: number;
        currentSegmentId: number | undefined;
        segmentCount: number;
      };
      db: {};
      ram: NodeJS.MemoryUsage;
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
  }>;
}
class TopicSerializer implements ISerializable {
  constructor(private topicFactory: ITopicFactory) {}
  serialize({ name, config }: ITopic<any>) {
    return { name, config };
  }
  deserialize(data: { name: string; config: ITopicConfig }) {
    return this.topicFactory.create(data.name, data.config);
  }
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
    private readonly storageService: IMessageStore<Data>,
    private readonly metrics: IMetricsCollector
  ) {}

  async getMetrics() {
    return {
      name: this.name,
      subscriptions: this.subscriptionService.getMetrics(),
      clients: this.clientService.getMetrics(),
      dlq: this.dlqService.getMetrics(),
      storage: await this.storageService.getMetrics(),
      ...this.ackService.getMetrics(),
      ...this.publishingService.getMetrics(),
      ...this.consumptionService.getMetrics(),
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
      consumerPendingThresholdMs: 10,
    },
    private codecFactory: new () => ICodec = ThreadedBinaryCodec,
    private queueFactory: new () => IPriorityQueue = BinaryHeapPriorityQueue,
    private storageFactory: new (
      ...args
    ) => IMessageStore<unknown> = LevelDBMessageStorage,
    private logService?: LogService
  ) {}

  create<Data>(name: string, config?: Partial<ITopicConfig>): Topic<Data> {
    // TODO: VALIDATE CONFIG AJV (add to registry)
    const mergedConfig = { ...this.defaultConfig, ...config };

    // const db = new Level('./mydb', {multithreading: true, compression: true, valueEncoding: 'buffer'}

    // new WriteAheadLog(path.join(topicDir, "wal.log"));
    // new MessageLog(path.join(topicDir, "segments"));
    // Level(path.join(topicDir, "metadata")); const topicDir = path.join(this.baseDir, this.topic);

    this.validateTopicName(name);
    const codec = new this.codecFactory();
    const logger = this.logService?.forTopic(name);

    // Build modules
    const messageStore = new this.storageFactory(
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
    const dlqManager = new DLQManager<Data>(name, messageStore, logger);
    const deliveryTracker = new DeliveryTracker(messageStore, metrics);

    const pendingAcks = new AckRegistry(
      clientManager,
      mergedConfig.consumerPendingThresholdMs
    );
    const subscriptionManager = new SubscriptionManager<Data>(
      clientManager,
      queueManager,
      messageStore,
      pendingAcks,
      deliveryTracker,
      logger
    );

    const messageRouter = new MessageRouter<Data>(
      clientManager,
      queueManager,
      subscriptionManager,
      dlqManager
    );
    const delayedManager = new DelayedMessageManager<Data>(
      new TimeoutScheduler<number>(new this.queueFactory()),
      messageStore,
      messageRouter,
      deliveryTracker,
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
      messageStore,
      pipeline,
      messageRouter,
      deliveryTracker,
      clientManager,
      delayedManager,
      metrics,
      logger
    );
    const consumptionService = new ConsumptionService(
      queueManager,
      messageStore,
      pendingAcks,
      deliveryTracker,
      clientManager,
      logger
    );
    const ackService = new AckService(
      pendingAcks,
      deliveryTracker,
      ackMonitor,
      messageStore,
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
type ITopicMetricType =
  | "totalMessagesPublished"
  | "totalBytes"
  | "ts"
  | "depth"
  | "enqueueRate"
  | "dequeueRate"
  | "avgLatencyMs"; // Time in queue
interface IMetricsCollector {
  recordEnqueue(byteSize: number, latencyMs: number): void;
  recordDequeue(latencyMs: number): void;
  getMetrics(): Record<ITopicMetricType, number>;
}
class TopicMetricsCollector implements IMetricsCollector {
  private metrics: IPersistedMap<ITopicMetricType, number>;

  constructor(mapFactory: IPersistedMapFactory) {
    this.metrics = mapFactory.create<ITopicMetricType, number>("metrics");
  }

  recordEnqueue(byteSize: number, latencyMs: number): void {
    const totalMessages = this.metrics.get("totalMessagesPublished") ?? 0;
    const totalBytes = this.metrics.get("totalBytes") ?? 0;
    const depth = this.metrics.get("depth") ?? 0;
    const enqueueRate = this.metrics.get("enqueueRate") ?? 0;
    this.metrics.set("totalMessagesPublished", totalMessages + 1);
    this.metrics.set("totalBytes", totalBytes + byteSize);
    this.metrics.set("depth", depth + 1);
    this.metrics.set("enqueueRate", enqueueRate + 1);
    this.updateAvgLatency(latencyMs);
  }

  recordDequeue(latencyMs: number): void {
    const depth = this.metrics.get("depth") ?? 0;
    const dequeueRate = this.metrics.get("dequeueRate") ?? 0;
    this.metrics.set("depth", depth - 1);
    this.metrics.set("dequeueRate", dequeueRate + 1);
    this.updateAvgLatency(latencyMs);
  }

  private updateAvgLatency(latencyMs: number) {
    const avgLatencyMs = this.metrics.get("avgLatencyMs") ?? 0;
    this.metrics.set("avgLatencyMs", avgLatencyMs * 0.9 + latencyMs * 0.1); // Exponential moving average
  }

  getMetrics() {
    return Object.fromEntries(this.metrics.entries()) as Record<
      ITopicMetricType,
      number
    >;
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
interface ISchemaValidator {
  (data: any): boolean;
}
class SchemaStore {
  private schemas: PersistedMap<string, JSONSchemaType<any>>;

  constructor(mapFactory: IPersistedMapFactory) {
    this.schemas = mapFactory.create("schemas");
  }

  get<Data>(schemaId: string): JSONSchemaType<Data> | undefined {
    return this.schemas.get(schemaId) as JSONSchemaType<Data>;
  }

  set(schemaId: string, schema: JSONSchemaType<any>): void {
    this.schemas.set(schemaId, schema);
  }

  delete(schemaId: string): void {
    this.schemas.delete(schemaId);
  }

  list(): string[] {
    return Array.from(this.schemas.keys());
  }
}
class ValidatorCache {
  private validatorCache = new Map<string, ISchemaValidator>();
  private ajv: Ajv;

  constructor(options?: AjvOptions) {
    this.ajv = new Ajv({
      allErrors: true,
      coerceTypes: false,
      useDefaults: true,
      code: { optimize: true, esm: true },
      ...options,
    });
  }

  getValidator(
    schemaId: string,
    schema: JSONSchemaType<any>
  ): ISchemaValidator {
    const cached = this.validatorCache.get(schemaId);
    if (cached) return cached;

    const validator = this.ajv.compile(schema);
    this.validatorCache.set(schemaId, validator);
    return validator;
  }

  removeValidator(schemaId: string): void {
    this.validatorCache.delete(schemaId);
  }

  clear(): void {
    this.validatorCache.clear();
  }
}
class SchemaVersionManager {
  constructor(
    private store: SchemaStore,
    private compatibilityMode: "none" | "forward" | "backward" | "full"
  ) {}

  register(name: string, schema: JSONSchemaType<any>): string {
    const latestKey = this.findLatestSchemaKey(name);

    if (latestKey && this.compatibilityMode !== "none") {
      const latestSchema = this.store.get(latestKey)!;

      if (!validateSchema(latestSchema, schema, this.compatibilityMode)) {
        throw new Error(
          `Schema ${name} is not compatible in ${this.compatibilityMode} mode`
        );
      }
    }

    const version = latestKey ? parseInt(latestKey.split(":")[1]) + 1 : 1;
    const schemaId = `${name}:${version}`;
    this.store.set(schemaId, schema);
    return schemaId;
  }

  findLatestSchemaKey(name: string): string | undefined {
    const candidates = this.store
      .list()
      .reduce<{ key: string; version: number }[]>((acc, key) => {
        if (!key.startsWith(name + ":")) return acc;
        return acc.concat({ key, version: parseInt(key.split(":")[1]) });
      }, [])
      .sort((a, b) => b.version - a.version);

    return candidates[0]?.key;
  }
}
class SchemaRegistry implements ISchemaRegistry {
  private store: SchemaStore;
  private validatorCache: ValidatorCache;
  private versionManager: SchemaVersionManager;

  constructor(
    mapFactory: IPersistedMapFactory,
    options?: AjvOptions,
    compatibilityMode: "none" | "forward" | "backward" | "full" = "backward"
  ) {
    this.store = new SchemaStore(mapFactory);
    this.validatorCache = new ValidatorCache(options);
    this.versionManager = new SchemaVersionManager(
      this.store,
      compatibilityMode
    );
  }

  register(name: string, schema: JSONSchemaType<any>): string {
    return this.versionManager.register(name, schema);
  }

  getValidator(schemaId: string): ISchemaValidator | undefined {
    const [name, versionStr] = schemaId.split(":");
    const version = parseInt(versionStr || "1");

    const storedSchema = this.store.get<unknown>(`${name}:${version}`);
    if (!storedSchema) return undefined;

    return this.validatorCache.getValidator(schemaId, storedSchema);
  }

  getSchema<Data>(schemaId: string): JSONSchemaType<Data> | undefined {
    const [name, versionStr] = schemaId.split(":");
    const version = versionStr ? parseInt(versionStr) : undefined;

    if (version) {
      return this.store.get(`${name}:${version}`);
    } else {
      const latestKey = this.versionManager.findLatestSchemaKey(name);
      return latestKey ? this.store.get(latestKey) : undefined;
    }
  }

  remove(schemaId: string): void {
    const [name, versionStr] = schemaId.split(":");
    const version = versionStr ? parseInt(versionStr) : undefined;

    if (version) {
      this.store.delete(`${name}:${version}`);
      this.validatorCache.removeValidator(`${name}:${version}`);
    } else {
      // Remove all versions
      for (const key of this.store.list()) {
        if (key.startsWith(`${name}:`)) {
          this.store.delete(key);
          this.validatorCache.removeValidator(key);
        }
      }
    }
  }

  listSchemas(): string[] {
    return Array.from(
      new Set(this.store.list().map((key) => key.split(":")[0]))
    ).map((name) => `${name}:latest`);
  }
}
interface ISchemaRegistry {
  register(name: string, schema: JSONSchemaType<any>): string;
  getValidator(schemaId: string): ISchemaValidator | undefined;
  getSchema<Data>(schemaId: string): JSONSchemaType<Data> | undefined;
  remove(schemaId: string): void;
  listSchemas(): string[];
}
interface ITopicRegistry {
  create<Data>(name: string, config: ITopicConfig): ITopic<Data>;
  get(name: string): ITopic<any> | undefined;
  delete(name: string): void;
  list(): MapIterator<string>;
}
class TopicRegistry implements ITopicRegistry {
  private topics: PersistedMap<string, ITopic<any>>;

  constructor(
    mapFactory: IPersistedMapFactory,
    private topicFactory: ITopicFactory,
    private logService?: ILogService
  ) {
    this.topics = mapFactory.create(
      "topics",
      new TopicSerializer(topicFactory)
    );
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
    if (!this.topics.has(name)) {
      throw new Error("Topic not found");
    }
    return this.topics.get(name);
  }

  delete(name: string): void {
    this.get(name);
    this.topics.delete(name);

    this.logService?.globalCollector.log("Topic deleted", { name });
  }
}

// delete meta keys + encode/decode pointer
// update topic config

// make all deletable/disposable
// separation of conserns, ioc(inversify_js), visualize components and flows
// project structure (save snapshot before)

// go to lmdb or redb, use transactional writes(exactly once feature)
// full Exactly-Once: deduplication(dedupId) & Consumer-side idempotent processing are DONE, Transactional Writes(Ensure state changes happen atomically), Broker-side state coordination(Coordinate with external systems)
// switch to Standalone server: you hit >50k msg/sec, cross-service/multi-lang support => need protobuf & lib/sdk per lang; for n-processes use proper-lockfile instead of custom Mutex

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

// ############################################## INTERNAL PARTITIONS #################################################################
// If you want even more throughput, partition each topic internally like Kafka-style partitions:
// Each partition gets its own queue, DLQ, delayed queue, etc. Consumers can subscribe to one or all partitions.
// You can route messages across partitions using consistent hashing or round-robin.

// ### 1. **Reduce Lock Contention**
// If all messages go into a single queue, every producer and consumer must coordinate access to that shared structure. Even in a single thread, this leads to:

// - Sequential enqueue/dequeue operations
// - Potential bottlenecks in high-throughput scenarios
// - Poor pipelining of async operations

// ### 2. **Granular Consumer Scaling**
// Partitioning allows consumers to scale independently per partition. For example:

// - One slow consumer only affects one partition.
// - Fast consumers can be assigned more partitions.
// - Load balancing becomes easier via consistent hashing or round-robin strategies.

// Even within a single thread, this enables better **pipelined execution** of async tasks like I/O, timers, and network calls.

// ### 3. **Improved Message Ordering Guarantees**
// If your system needs strict ordering **per key** (e.g., per user or per session), partitioning by key ensures:

// - All messages for a given key go to the same partition
// - Consumers process them in order within that partition
// - No global lock required

// Without partitioning, you'd need expensive synchronization mechanisms to preserve order globally.

// ### 4. **Better Utilization of I/O-Bound Workloads**
// Node.js excels at handling **I/O-bound** workloads (network, disk, timers). Partitions allow overlapping:

// - Multiple producers writing to different partitions
// - Consumers reading from different partitions concurrently (via event loop)
// - Internal DLQs, delayed queues, etc., operating independently

// This maximizes throughput under asynchronous I/O pressure.

// ### 5. **Future-Proofing for Multi-Threading**
// Even if you start with a single-threaded model, designing with partitions in mind makes it easy to later:

// - Move each partition to a separate `worker_thread`
// - Distribute partitions across machines
// - Implement sharding patterns similar to Kafka

// ### Step 1: Add Partition Count to Topic Config
// ```ts
// interface ITopicConfig<Data> {
//   schema?: JSONSchemaType<Data>;
//   persist?: boolean;
//   retentionMs?: number;
//   maxDeliveryAttempts?: number;
//   maxMessageSize?: number;
//   partitions?: number; // New field
// }
// ```

// ### Step 2: Create Partitioned Queues in `TopicQueueManager`
// ```ts
// class TopicQueueManager {
//   private partitionQueues: IPriorityQueue[];
//   private routingStrategy: IRoutingStrategy;

//   constructor(
//     routingStrategy: IRoutingStrategy,
//     private queueFactory: () => IPriorityQueue<Buffer>,
//     private partitionCount: number
//   ) {
//     this.partitionQueues = Array.from({ length: partitionCount }, queueFactory);
//     this.routingStrategy = routingStrategy;
//   }

//   getQueue(key?: string): IPriorityQueue {
//     if (!key) return this.partitionQueues[0]; // Fallback

//     const hash = this.routingStrategy.getConsumerId(key); // or custom partitioner
//     const partitionIndex = Math.abs(hash!) % this.partitionCount;
//     return this.partitionQueues[partitionIndex];
//   }
// }
// ```

// ### Step 3: Modify `Topic` Class to Accept Partitions
// ```ts
// class Topic<Data> {
//   private readonly queues: TopicQueueManager;

//   constructor(
//     public name: string,
//     private context: IContext,
//     routingStrategy: IRoutingStrategy,
//     private config?: ITopicConfig<Data>
//   ) {
//     const partitionCount = config?.partitions || 1;
//     this.queues = new TopicQueueManager(routingStrategy, context.queueFactory, partitionCount);
//   }
// }
// ```

// ---

// ## 🧱 Step 2: Add Internal Topic Partitioning

// You already have a `TopicQueueManager`. We'll extend it to support internal partitioning.

// ### 🔧 Update `ITopicConfig`
// ```ts
// // topics/types.ts
// interface ITopicConfig<Data> {
//   schema?: JSONSchemaType<Data>;
//   persist?: boolean;
//   retentionMs?: number;
//   archivalThreshold?: number;
//   maxSizeBytes?: number;
//   maxDeliveryAttempts?: number;
//   maxMessageSize?: number;
//   partitions?: number; // add this
// }
// ```

// ### 🔧 Modify `TopicQueueManager` to Support Partitions
// ```ts
// // topics/queue-manager.ts
// class TopicQueueManager {
//   private sharedQueue: IPriorityQueue;
//   private unicastQueues = new Map<number, IPriorityQueue>();
//   private partitionQueues: IPriorityQueue[];

//   constructor(
//     private routingStrategy: IRoutingStrategy,
//     private queueFactory: () => IPriorityQueue<Buffer>,
//     private partitionCount: number = 1
//   ) {
//     this.sharedQueue = this.queueFactory();
//     this.partitionQueues = Array.from({ length: partitionCount }, queueFactory);
//   }

//   getPartitionIndex(key: string): number {
//     const hash = this.routingStrategy.getConsumerId(key); // or custom hasher
//     return Math.abs(hash!) % this.partitionQueues.length;
//   }

//   getQueue(key?: string): IPriorityQueue {
//     if (key) {
//       const targetConsumer = this.routingStrategy.getConsumerId(key);
//       const unicastQueue = this.unicastQueues.get(targetConsumer);
//       if (unicastQueue) return unicastQueue;

//       const partitionIdx = this.getPartitionIndex(key);
//       return this.partitionQueues[partitionIdx];
//     }
//     return this.sharedQueue;
//   }

//   addConsumerQueue(consumerId: number): void {
//     this.unicastQueues.set(consumerId, this.queueFactory());
//   }

//   removeConsumerQueue(consumerId: number): void {
//     this.unicastQueues.delete(consumerId);
//   }

//   enqueue(record: Buffer, meta: Metadata): void {
//     const queue = this.getQueue(meta.correlationId);
//     queue.enqueue(record, meta.priority);
//   }

//   dequeue(consumerId: number): Buffer | undefined {
//     const queue = this.unicastQueues.get(consumerId);
//     if (queue?.size()) return queue.dequeue();
//     return this.sharedQueue.dequeue();
//   }
// }
// ```

// ### ✅ Update `Topic` Constructor
// ```ts
// // topics/topic.ts
// constructor(
//   public name: string,
//   private context: IContext,
//   routingStrategy: IRoutingStrategy,
//   private config?: ITopicConfig<Data>
// ) {
//   const partitionCount = config?.partitions || 1;
//   this.queues = new TopicQueueManager(routingStrategy, context.queueFactory, partitionCount);
//   ...
// }
// ```

// ## 🚫 So Why Do We Even Have Kafka-Style Partitions?

// Kafka-style partitions exist because:
// - Topics are high-throughput. It supports **millions of messages/sec**
// - Consumers don't care about strict ordering per key
// - Partitions allow horizontal scaling within a single topic
// - Producers write to any partition
// - Consumers read from one or more partitions

// But these features come at the cost of complexity:
// - Rebalancing logic
// - Offset management
// - Partition assignment strategies

// But in your case:
// - You **do** care about ordering per key
// - You’re not trying to maximize throughput at all costs
// - You prefer simplicity over scale-at-all-costs

// So:
// ❌ If you don’t need that level of scale, they’re overkill.

// ############################################## LOG BASED QUEUE #################################################################

// class OffsetTracker {
//   constructor(private storage: IStorage, private topicName: string) {}

//   async getOffset(consumerId: number): Promise<number> {
//     const key = `offset:${this.topicName}:${consumerId}`;
//     const buffer = await this.storage.get(this.topicName, key);
//     return buffer ? buffer.readUInt32BE(0) : 0;
//   }

//   async commitOffset(consumerId: number, offset: number): Promise<void> {
//     const key = `offset:${this.topicName}:${consumerId}`;
//     const buffer = Buffer.alloc(4);
//     buffer.writeUInt32BE(offset, 0);
//     await this.storage.put(this.topicName, key, buffer);
//   }
// }

// class Topic<Data> {
//   private readonly offsetTracker: OffsetTracker;

//   constructor(
//     public name: string,
//     private context: IContext,
//     routingStrategy: IRoutingStrategy,
//     private config?: ITopicConfig<Data>
//   ) {
//     this.offsetTracker = new OffsetTracker(context.storage, name);
//     // ...
//   }

//   async send(record: Buffer, meta: Metadata): Promise<void> {
//     if (await this.pipeline.process(record, meta)) return;

//     // Write once to shared log
//     const offset = await this.getNextOffset();
//     await this.context.storage.put(this.name, `msg:${offset}`, record);

//     this.metrics.recordMessagePublished(meta);
//   }

//   private async getNextOffset(): Promise<number> {
//     const key = `__topic_offset__:${this.name}`;
//     let current = 0;
//     try {
//       const buffer = await this.context.storage.get(this.name, key);
//       current = buffer ? buffer.readUInt32BE(0) : 0;
//     } catch {
//       /* ignore */
//     }
//     const next = current + 1;
//     const buffer = Buffer.alloc(4);
//     buffer.writeUInt32BE(next, 0);
//     await this.context.storage.put(this.name, key, buffer);
//     return next;
//   }

//   async consume(consumerId: number): Promise<[Data, Metadata] | undefined> {
//     const offset = await this.offsetTracker.getOffset(consumerId);
//     const key = `msg:${offset}`;
//     const record = await this.context.storage.get(this.name, key);
//     if (!record) return;

//     const message = this.context.codec.decode<Data>(record);
//     if (await this.pipeline.process(record, message[1])) return;

//     // Advance offset only after successful processing
//     await this.offsetTracker.commitOffset(consumerId, offset + 1);

//     return message;
//   }
// }
// ```

// ### USER
// now it is make sense, but for every consume we need to scan offset-next messages with decoding all including those we will skip. Is it similar to consumer groups? cpu greedy consumption just for not duplicate messages per consumer (ram/disk optimization). Is it worth? Another approach: store log as map and use queues per consumer which store only messageId. Put message in log and fan out id to related consumer queues. Which approach is better for u?
// ### USER
// i like the log-scan approach, its smart and we can make codec decode only one metadata field (tags in this case) due to preposition. but in scale i think its may be not so fast like messageId fanout approach, especially in edge cases. also in second approach we can use buffer as queue because messageId is a number. Why kafka doesnt use this?
// ### USER
// Job queues and eventLog have distinct arch and seems like this lib should be doing one thing eventually.
// I think i dont want kafka streaming. I just want a scallable message broker which is more like Rabbitmq i guess.
// RabbitMq analogue: log is an exchange, offsets are binary queues(with messageId if log is map or messageIndex if log is list) binded to exchange but ram effective
// Message marks will be deleted from the offsets on ack, if nack - also offset manipulation
// But we need both routing and consistency hashing (correlationId) implementation here
// What do u think?

// ############################################## ROAD TO SERVER #################################################################

// ### USER
// If it goes to the standalone nodejs server how it will be scalled?

// ## 🔍 TL;DR: Scaling Strategy for Your Standalone Broker

// | Aspect | Strategy |
// |-------|----------|
// | Horizontal Scale | Use topic partitioning + consumer groups |
// | Fault Tolerance | Add replication & WAL failover |
// | Cross-Language Support | Expose gRPC or HTTP streaming API |
// | Scalable Consumers | Use offset tracking + consumer groups |
// | High Throughput | Offload CPU work (codec) to worker pool |
// | Persistence | Keep LevelDB/WAL + optional Raft/RocksDB later |
// | Cluster Coordination | Optional – use Redis, etcd, or Raft |

// ---

// ## ✅ Step-by-Step: From Embedded Lib to Standalone Broker

// ### 1. 📦 Package Structure

// Split the codebase into microservices-style layers:

// ```
// broker-server/
// ├── core/               # Core logic (same as lib)
// ├── api/                # REST/gRPC interface
// ├── persistence/        # LevelDB/WAL/RocksDB
// ├── cluster/            # Optional coordination layer
// ├── cli/                # CLI tool for management
// └── main.ts             # Entry point
// ```

// ### 3. 🧩 Define External Interface (gRPC Example)

// ```protobuf
// // proto/broker.proto
// syntax = "proto3";

// package broker;

// service TopicService {
//   rpc Produce(stream ProducerRequest) returns (ProducerResponse);
//   rpc Consume(ConsumerRequest) returns (stream ConsumerResponse);
//   rpc Ack(AckRequest) returns (AckResponse);
//   rpc ReplayDlq(DlqReplayRequest) returns (DlqReplayResponse);
// }

// message ProducerRequest {
//   string topic = 1;
//   bytes data = 2;
//   map<string, string> metadata = 3;
// }

// message ProducerResponse {
//   repeated int64 ids = 1;
// }

// message ConsumerRequest {
//   string topic = 1;
//   int32 consumer_id = 2;
//   int32 limit = 3;
//   bool auto_ack = 4;
// }

// message ConsumerResponse {
//   int64 id = 1;
//   bytes data = 2;
//   map<string, string> metadata = 3;
// }

// message AckRequest {
//   string topic = 1;
//   repeated int64 ids = 2;
// }
// ```

// ---

// ### 4. ⚙️ Scaling Internally in Node.js

// Once it’s a server, you can scale using Node.js-native tools:

// #### ✅ A. Multi-Core Parallelism with `cluster.fork()`

// Use Node.js’ `cluster` module to run each topic on a separate process.

// ```ts
// import cluster from 'cluster';
// import os from 'os';

// if (cluster.isPrimary) {
//   const numCPUs = os.cpus().length;
//   for (let i = 0; i < numCPUs; i++) {
//     cluster.fork();
//   }
// } else {
//   // Worker process runs the actual broker
//   startBroker();
// }
// ```

// Each topic can be assigned to a thread via consistent hashing of topic name.

// ### 9. 🪶 Lightweight Architecture Benefits

// Even though it's a single server, you can still scale elegantly:

// | Feature | Strategy |
// |--------|----------|
// | Single-threaded limits | Move to `cluster` or `worker_threads` |
// | Consumer concurrency | Assign N consumers per topic |
// | Message order guarantees | Sticky routing via key hash |
// | Fan-out efficiency | Broadcast log + virtual offsets |
// | Backpressure | Rate limiting + buffer control |
// | Debugging | Metrics + tracing via OpenTelemetry |

// ---

// ### 1. **Isolate High-Traffic Topics into Dedicated Workers**

// You can create a `TopicWorkerPool` that assigns certain topics to dedicated threads:

// ```ts
// class TopicWorkerPool {
//   private workers = new Map<string, Worker>();
//   constructor(private topicNames: string[]) {
//     topicNames.forEach((name) => {
//       this.workers.set(name, new Worker(`./topic-worker.js`, { env: { TOPIC: name } }));
//     });
//   }

//   get(topicName: string): Worker {
//     return this.workers.get(topicName)!;
//   }
// }
// ```

// Each worker handles its own queue, consumer routing, and persistence.
