import type { JSONSchemaType } from "ajv";

export type ISchemaDefinition<T = any> = JSONSchemaType<T>; // to extend
