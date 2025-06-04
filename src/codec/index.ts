export { BinarySchemaCompiler } from "./schema/compiler";
export { PrecompiledSchema } from "./schema/precompiled";
export { BinarySchema } from "./schema/schema";
export {
  CodecWorkerRouter as default,
  type ICodecWorkerRouter as ICodec,
} from "./worker_router";

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
