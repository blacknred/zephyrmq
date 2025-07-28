import type { ISchemaCreator } from "@domain/interfaces/schema/ISchemaCreator";
import type { ISchemaDeleter } from "@domain/interfaces/schema/ISchemaDeleter";
import type { ISchemaGetter } from "@domain/interfaces/schema/ISchemaGetter";
import type { ISchemaLister } from "@domain/interfaces/schema/ISchemaLister";

export interface ISchemaRegistry
  extends ISchemaCreator,
    ISchemaGetter,
    ISchemaLister,
    ISchemaDeleter {}
