import type { IEntriesReader } from "@zephyrmq/mapstore/src/domain/interfaces/IEntriesReader";
import type { IKeysReader } from "@zephyrmq/mapstore/src/domain/interfaces/IKeysReader";
import type { IValuesReader } from "@zephyrmq/mapstore/src/domain/interfaces/IValuesReader";
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
