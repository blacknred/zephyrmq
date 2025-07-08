import type { IKeyTracker } from "@domain/interfaces/IKeyTracker";
import { CuckooFilter } from "bloom-filters";

export class CuckooFilterKeyTracker<K> implements IKeyTracker<K> {
  private filter: CuckooFilter;
  constructor(
    private expectedItems = 1_000_000,
    private errorRate = 0.001
  ) {
    this.filter = CuckooFilter.create(expectedItems, errorRate);
  }

  clear() {
    this.filter = CuckooFilter.create(this.expectedItems, this.errorRate);
  }

  has(element: K): boolean {
    return this.filter.has(`${element}`);
  }

  remove(element: K): boolean {
    return this.filter.remove(`${element}`);
  }

  add(element: K): boolean {
    return this.filter.add(`${element}`);
  }

  get length() {
    return this.filter.length;
  }
}
