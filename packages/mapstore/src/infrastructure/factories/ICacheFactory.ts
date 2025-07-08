import type { IEntriesReader } from "../../domain/interfaces/IEntriesReader";
import type { IKeysReader } from "../../domain/interfaces/IKeysReader";
import type { IValuesReader } from "../../domain/interfaces/IValuesReader";
import type { FifoCache } from "@infra/cache/fifo/FifoCache";

export interface ICacheFactory {
  create<K extends string | number, V>(
    bytesPerEntry?: number
  ): {
    cache: FifoCache<K, V>;
    cacheEnriesReader: IEntriesReader<K, V>;
    cacheKeysReader: IKeysReader<K>;
    cacheValuesReader: IValuesReader<V>;
  };
}
