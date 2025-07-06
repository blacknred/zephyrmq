
// import type { ISerializable } from "@domain/ports/ISerializable";

// export class QueueSerializer<T> implements ISerializable {
//   constructor(
//     private queueFactory: IQueueFactory,
//     private keyRetriever: (entry: T) => string | number
//   ) {}
//   serialize(_: unknown, key: string) {
//     return key;
//   }
//   deserialize(name: string) {
//     return this.queueFactory.create(`queue!${name}`, this.keyRetriever);
//   }
// }
