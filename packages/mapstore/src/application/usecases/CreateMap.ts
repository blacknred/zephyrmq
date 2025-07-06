import type { IMapFactory, IMapOptions } from "@app/interfaces/IMapFactory";

export class CreateMap {
  constructor(private readonly mapCreator: IMapFactory) {}

  execute<K extends string | number, V>(
    name: string,
    options?: IMapOptions<V>
  ) {
    return this.mapCreator.create<K, V>(name, options);
  }
}
