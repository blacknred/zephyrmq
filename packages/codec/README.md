![TypeScript](https://shields.io/badge/TypeScript-3178C6?logo=TypeScript&logoColor=FFF&style=flat-square)
![Node.js](https://shields.io/badge/Node.js-417e38?logo=nodedotjs&logoColor=FFF&style=flat-square)
[![Build Status](https://github.com/blcknrd/mbroker/workflows/Code%20quality%20checks/badge.svg)](https://github.com/blcknrd/mbroker/actions)

# @zephyrmq/codec

Serializer, Compressor, Encryptor pipeline processor

SchemaBinaryCodec



SchemaRegistry

BinarySchemaRegistry
BinaryCodec.encode.encodeWithSchema

Broker[TopicRegistry, SchemaDefinitionRegistry(json schema), SchemaValidatorRegistry]
Encoder[SchemaSerializerRegistry, Codec]
SchemaDefRegistry=> [SchemaValidatorRegistry, SchemaRegistry]

/**
 * BinaryCodec
 *
 * Features
 * Speed - Faster encode/decode than JSON or Protobuf - No parsing overhead; uses direct buffer writes
 * Size - Smaller payloads than JSON - Efficiently encodes only values + bit-packed flags for optional fields
 * Schema-driven - Type-safe, structured data - Enforces field types and layout at runtime, can be compiled from JSON schema
 * Compression - Easily extendable - Uses snappy for high speed, optional use
 * Memory Efficiency - Low GC pressure - Uses preallocated buffers
 * Encryption Ready - Integrates cleanly with AES-GCM or similar - Optional use
 * CRC Integrity - Detect corruption - Add checksums for safety
 * Worker-Friendly - Optimized for NodeJS multi-threaded use - Transferrable Buffers(UInt8Array conversion), cached schemas in Workers
 *
 * Comparison
 * Vs json: -50% ram, 3x encode, 5x decode
 * Vs protobuf: up to 2x encode (if used with AJV for JSON schema validation), -10% size, -40% ram; no cross-lang, no schema-evolution, no versioning
 */

// For frequent small-object R/W, MessagePack in the main thread is faster and simpler.
// Workers introduce IPC delays that outweigh benefits for (<10KB). Same with compression.



// const codec = new BinaryCodec();

// type IUser = {
//   id: number;
//   name: string;
//   email?: string;
//   age: number;
// };
// const userJsonSchema = {
//   type: "object",
//   properties: {
//     id: { type: "integer" },
//     name: { type: "string" },
//     email: { type: "string" },
//     age: { type: "integer" },
//   },
//   required: ["id", "name", "age"],
// };
// const userSchema = compileSchemaFromJson<IUser>(userJsonSchema);
// const userBinSchema = createBinarySchema(userSchema);
// const userBinSchema1 = createBinarySchema<IUser>({
//   id: { type: "uint32" },
//   name: { type: "string" },
//   email: { type: "string", optional: true },
//   age: { type: "uint8" },
// });

// (async () => {
//   const user = {
//     id: 1,
//     name: "Alice",
//     email: "alice@example.com",
//     age: 30,
//   };
//   const buffer = await codec.encode(user, userBinSchema);
//   const decodedUser = await codec.decode(buffer, userBinSchema);
// })();