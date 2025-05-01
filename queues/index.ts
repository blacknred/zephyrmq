// HeapQueue is 3-4x faster at scale due to O(log n) vs. O(n) inserts
// LinkedListQueue struggles with priorities (sorts on every insert).
// Memory usage is similar (~10MB for 1M messages).

export { HighCapacityBinaryHeapPriorityQueue } from "./binary_heap_priority_queue";
export { LinkedListPriorityQueue } from "./linked_list_proirity_queue";
