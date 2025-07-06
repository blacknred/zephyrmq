import type { IPriorityQueue } from "./IPriorityQueue";

export interface IQueue<T> extends IPriorityQueue<T> {
  flush(): Promise<void>;
  clear(): void;
}
