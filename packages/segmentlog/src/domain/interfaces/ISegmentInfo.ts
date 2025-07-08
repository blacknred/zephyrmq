import fs from "node:fs/promises";

export interface ISegmentInfo {
  id: number;
  filePath: string;
  indexFilePath: string;
  baseOffset: number;
  lastOffset: number;
  size: number;
  recordCount: number;
  fileHandle?: fs.FileHandle;
}
