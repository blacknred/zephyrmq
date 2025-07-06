import type { IValuesReader } from "@domain/ports/IValuesReader";

export class ReadValues<V> {
  constructor(private readonly valuesReader: IValuesReader<V>) {}

  execute() {
    return this.valuesReader.values();
  }
}
