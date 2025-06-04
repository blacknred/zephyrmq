import type { IFieldDefinition, ISchema } from "./types";

export class BinarySchemaSizer {
  static estimateSize<T>(data: T, schema: ISchema<T>): number {
    let total = 0;
    const entries = Object.entries(schema) as Array<
      [keyof T, IFieldDefinition]
    >;

    for (const [key, def] of entries) {
      const value = data[key];
      if (def.optional && value === undefined) continue;

      total += this.getFieldSize(def, value);
    }

    const flagByte = Object.values(schema).some((def) => def.optional) ? 1 : 0;
    return flagByte + total;
  }

  static getFieldSize(def: IFieldDefinition, value: any): number {
    switch (def.type) {
      case "int8":
      case "uint8":
        return 1;

      case "int16":
      case "uint16":
        return 2;

      case "int32":
      case "uint32":
        return 4;

      case "double":
        return 8;

      case "string":
        return 2 + Buffer.byteLength(value, "utf8");

      case "buffer":
        return 2 + value.length;

      case "array": {
        let size = 2; // length prefix
        for (const item of value) {
          size += this.getFieldSize(def.itemType!, item);
        }
        return size;
      }

      case "object": {
        let size = 0;
        for (const [key, fieldDef] of Object.entries(def.schema!)) {
          const fieldValue = value[key];
          size += this.getFieldSize(fieldDef, fieldValue);
        }
        return size;
      }

      default:
        throw new Error(`Unknown field type: ${def.type}`);
    }
  }
}
