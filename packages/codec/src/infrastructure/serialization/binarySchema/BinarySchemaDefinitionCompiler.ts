import type { FieldType, IFieldDefinition, ISchemaDefinition } from "./BinarySchema";

export class BinarySchemaDefinitionCompiler {
  static fromJsonSchema<T>(jsonSchema: Record<string, any>): ISchemaDefinition<T> {
    if (!jsonSchema.properties) {
      throw new Error("Schema must include 'properties'");
    }

    const result: Record<string, IFieldDefinition> = {};

    for (const [key, prop] of Object.entries(jsonSchema.properties)) {
      const def = this.parseField(prop);
      def.optional = !jsonSchema.required?.includes(key);
      result[key] = def;
    }

    return result as ISchemaDefinition<T>;
  }

  private static parseField(prop: any): IFieldDefinition {
    const type = prop.type;

    // Handle oneOf/discriminator (polymorphic types)
    if (prop.oneOf && prop.discriminator?.propertyName) {
      return {
        type: "object",
        schema: {
          [prop.discriminator.propertyName]: {
            type: "string",
          },
          // You'd need additional mapping here based on each oneOf branch
        },
        optional: false,
      };
    }

    // Nested object
    if (type === "object" && prop.properties) {
      return {
        type: "object",
        schema: Object.entries(prop.properties).reduce(
          (acc: Record<string, IFieldDefinition>, [key, val]) => {
            acc[key] = this.parseField(val);
            return acc;
          },
          {}
        ),
        optional: false,
      };
    }

    // Array
    if (type === "array" && prop.items) {
      return {
        type: "array",
        itemType: this.parseField(prop.items),
        optional: false,
      };
    }

    // Boolean
    if (type === "boolean") {
      return {
        type: "boolean",
        optional: false,
      };
    }

    // Null
    if (type === "null") {
      return {
        type: "null",
        optional: true,
      };
    }

    // Date/time
    if (type === "string" && prop.format === "date-time") {
      return {
        type: "timestamp",
        optional: false,
      };
    }

    // Primitive types
    let fieldType: FieldType = "string";
    switch (type) {
      case "integer":
        fieldType = "int32";
        break;
      case "number":
        fieldType = "double";
        break;
      case "string":
        fieldType = "string";
        break;
      default:
        throw new Error(`Unsupported type: ${type}`);
    }

    return { type: fieldType };
  }
}
