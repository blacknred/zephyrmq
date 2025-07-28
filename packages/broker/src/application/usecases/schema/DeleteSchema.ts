import type { ILogService } from "@app/interfaces/ILogService";
import type { ISchemaDeleter } from "@domain/interfaces/schema/ISchemaDeleter";
import type { ISchemaLister } from "@domain/interfaces/schema/ISchemaLister";
import type { ISchemaValidator } from "@domain/interfaces/schema/ISchemaValidator";
import type { ITopicDeleter } from "@domain/interfaces/topic/ITopicDeleter";
import type { ICodec } from "@zephyrmq/codec";

export class DeleteSchema {
  constructor(
    private deleter: ISchemaDeleter,
    private lister: ISchemaLister,
    private codec: ICodec,
    private schemaValidators: Map<string, ISchemaValidator>,
    private logService?: ILogService
  ) {}

  async execute(schemaId: string) {
    const [name, version] = schemaId.split(":");
    // const version = versionStr ? parseInt(versionStr) : undefined;


    if (version) {
      // const fullName = `${name}:${version}`;
      this.deleter.delete(schemaId);
      this.schemaValidators.delete(schemaId);
      await this.codec.removeSchema(schemaId);

      this.logService?.log("Topic is deleted", { schemaId });
      return;
    }

    const versions

      // Remove all versions
      for (const key of this.lister.list()) {
        if (key.startsWith(`${name}:`)) {
          this.deleter.delete(key);
          this.schemaValidators.delete(key);
          await this.codec.removeSchema(key);
        }
      }
    
  }

    // this.deleter.delete(schemaId);

    // await this.codec.removeSchema(schemaId);

    // this.logService?.log("Topic is deleted", { schemaId });
  }
}


  //   const [name, versionStr] = schemaId.split(":");
  //   const version = versionStr ? parseInt(versionStr) : undefined;

  //   if (version) {
  //     const fullName = `${name}:${version}`;
  //     this.store.delete(fullName);
  //     this.validatorCache.removeValidator(fullName);
  //     await this.codec.removeSchema(fullName);
  //   } else {
  //     // Remove all versions
  //     for (const key of this.store.list()) {
  //       if (key.startsWith(`${name}:`)) {
  //         this.store.delete(key);
  //         this.validatorCache.removeValidator(key);
  //         await this.codec.removeSchema(key);
  //       }
  //     }
  //   }
  // }