// types/json-schema-compatibility.d.ts

declare module "json-schema-compatibility" {
  /**
   * Validates whether newSchema is backward compatible with oldSchema.
   */
  export function validateSchema(
    oldSchema: JSONSchemaType<unknown>,
    newSchema: JSONSchemaType<unknown>,
    mode?: "none" | "forward" | "backward" | "full"
  ): boolean;

  /**
   * Checks if two schemas are identical (useful for idempotent updates).
   */
  export function equals(oldSchema: JSONSchemaType<unknown>, newSchema: JSONSchemaType<unknown>): boolean;
}