// import { MessageMetadata } from "..";

// class JSONCodec {
//   encode<T>(data: T, meta: MessageMetadata) {
//     try {
//       // 1. Build the complete object
//       const message = { "0": data };
//       meta.keys.forEach((k, i) => {
//         if (meta[k] !== undefined) message[i + 1] = meta[k];
//       });

//       // 2. Pre-allocation reduces GC pressure
//       const jsonString = JSON.stringify(message);
//       const buffer = Buffer.allocUnsafe(Buffer.byteLength(jsonString));

//       // 3. Single write operation
//       buffer.write(jsonString, 0, "utf8");
//       return buffer;
//     } catch (e) {
//       throw new Error("Failed to encode message");
//     }
//   }

//   decode<T>(buffer: Buffer): [T, MessageMetadata] {
//     try {
//       // 1. Fast path for empty/small buffers
//       if (buffer.length < 2) throw new Error("Invalid message");

//       // 2. Single string conversion
//       const str =
//         buffer.length < 4096
//           ? buffer.toString("utf8") // Small buffers
//           : Buffer.prototype.toString.call(buffer, "utf8"); // Large buffers avoids prototype lookup

//       // 3. Parse with reviver for direct metadata mapping
//       const meta = new MessageMetadata();
//       const data = JSON.parse(str, (k, v: number | string) => {
//         if (k === "0") return v; // Return payload as-is
//         const metaIndex = parseInt(k, 10) - 1;
//         if (!isNaN(metaIndex)) {
//           const metaKey = meta.keys[metaIndex];
//           // @ts-ignore
//           if (metaKey) meta[metaKey] = v;
//         }
//         return;
//       }) as T;

//       return [data, meta];
//     } catch (e) {
//       throw new Error("Failed to decode message");
//     }
//   }
// }
