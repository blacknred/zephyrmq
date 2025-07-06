import type { IQueue } from "../../../../../broker/src/infrastructure/queue/IQueue";
import type { IStructureOptions } from "./IStructureOptions";

export interface IQueueOptions<T> extends IStructureOptions<T> {
  keyRetriever?: (entry: T) => string;
}

export interface IQueueCreator {
  createQueue<T>(name: string, options?: IQueueOptions<T>): IQueue<T>;
}
