export interface ISchema<T> {
  serialize(data: T): Buffer;
  deserialize(buffer: Buffer): T;
}

export interface IFieldDefinition {
  type: FieldType;
  optional?: boolean;
  itemType?: IFieldDefinition; // For arrays
  schema?: Record<string, IFieldDefinition>; // For nested objects
}

export type FieldType =
  | "int8"
  | "uint8"
  | "int16"
  | "uint16"
  | "int32"
  | "uint32"
  | "double"
  | "string"
  | "buffer"
  | "array"
  | "object"
  | "boolean"
  | "timestamp" // Unix timestamp (seconds)
  | "null";

export type Schema<T> = {
  [K in keyof T]: IFieldDefinition;
};
