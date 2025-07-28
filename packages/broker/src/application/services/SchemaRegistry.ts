import type { ISchemaRegistry } from "@app/interfaces/ISchemaRegistry";
import type { CreateSchema } from "@app/usecases/schema/CreateSchema";
import type { DeleteSchema } from "@app/usecases/schema/DeleteSchema";
import type { GetSchema } from "@app/usecases/schema/GetSchema";
import type { ListSchemas } from "@app/usecases/schema/ListSchemas";
import type { ISchema } from "@domain/interfaces/schema/ISchema";
import type { ISchemaDefinition } from "@domain/interfaces/schema/ISchemaDefinition";

export class SchemaRegistry implements ISchemaRegistry {
  // private store: ISchemaDefStore;
  // private validatorCache: IValidatorCache;
  // private versionManager: ISchemaDefVersionManager;
  // private logger: ILogCollector;

  constructor(
    private createSchema: CreateSchema,
    private getSchema: GetSchema,
    private listSchemas: ListSchemas,
    private deleteSchema: DeleteSchema
    // mapFactory: IPersistedMapFactory,
    // options?: AjvOptions,
    // compatibilityMode: "none" | "forward" | "backward" | "full" = "backward"
  ) {
    // this.logger = logService.globalCollector;
    // this.store = new SchemaDefStore(mapFactory);
    // this.validatorCache = new ValidatorCache(options);
    // this.versionManager = new SchemaDefVersionManager(
    //   this.store,
    //   compatibilityMode
    // );
  }

  async create(
    name: string,
    schemaDef: ISchemaDefinition,
    author?: string
  ): Promise<string | void> {
    return this.createSchema.execute(name, schemaDef, author);
  }

  async get(schemaId: string): Promise<ISchema<any> | undefined> {
    return this.getSchema.execute(schemaId);
  }

  async list(): Promise<string[]> {
    return this.listSchemas.execute();
  }

  async delete(schemaId: string): Promise<void> {
    return this.deleteSchema.execute(schemaId);
  }
}

// async getValidator(schemaId: string): ISchemaValidator | undefined {
//   const [name, versionStr] = schemaId.split(":");
//   const version = parseInt(versionStr || "1");

//   const storedSchema = this.store.get<unknown>(`${name}:${version}`);
//   if (!storedSchema) return undefined;

//   return this.validatorCache.getValidator(schemaId, storedSchema);
// }
