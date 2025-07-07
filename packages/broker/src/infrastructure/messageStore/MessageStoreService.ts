// import type { MessageMetadata } from "@domain/entities/MessageMetadata";

// export class MessageStoreService<Data> implements IMessageStore<Data> {
//   constructor(
//     private replayer: IWALReplayer,
//     private writer: IMessageWriter,
//     private reader: IMessageReader<Data>,
//     private retentionManager: IMessageRetentionManager,
//     private db: Level<string, Buffer>,
//     private wal: IWriteAheadLog,
//     private log: IMessageLog
//   ) {
//     this.init();
//   }

//   private async init() {
//     await this.replayer.replay();
//     this.retentionManager.start();
//   }

//   async close() {
//     this.retentionManager.stop();
//     await Promise.all([this.wal.close(), this.db.close()]);
//   }

//   async getMetrics() {
//     const [walStats, logStats] = await Promise.all([
//       this.wal.getMetrics(),
//       this.log.getMetrics(),
//     ]);

//     return {
//       wal: walStats,
//       log: logStats,
//       db: {
//         /** db.stats() is not implemented in Level */
//       },
//       ram: process.memoryUsage(),
//     };
//   }






//   async write(
//     message: Buffer,
//     meta: MessageMetadata
//   ): Promise<number | undefined> {
//     return this.writer.write(message, meta);
//   }

//   async read(id: number) {
//     return this.reader.read(id);
//   }

//   async readMessage(id: number): Promise<Data | undefined> {
//     return this.reader.readMessage(id);
//   }

//   async readMetadata<K extends keyof MessageMetadata>(
//     id: number,
//     keys?: K[]
//   ): Promise<Pick<MessageMetadata, K> | undefined> {
//     return this.reader.readMetadata(id, keys);
//   }

//   async markDeletable(id: number): Promise<void> {
//     return this.retentionManager.markDeletable(id);
//   }

//   async unmarkDeletable(id: number): Promise<void> {
//     return this.retentionManager.markDeletable(id);
//   }


// }
