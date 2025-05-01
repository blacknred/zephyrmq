export class HighCapacityBinaryHeapPriorityQueue<Data = any> {
  public heap: Array<{ value: Data; priority: number }>;
  private count = 0;
  private freeSlots: number[] = []; // Track recycled array positions

  constructor(initialCapacity = 1000000) {
    this.heap = new Array(initialCapacity);
  }

  // Private methods with direct index calculations
  private parent(i: number) {
    return (i - 1) >> 1; // Math.floor((i-1)/2)
  }
  private left(i: number) {
    return (i << 1) + 1; // 2*i + 1
  }
  private right(i: number) {
    return (i << 1) + 2; // 2*i + 2
  }
  private swap(i: number, j: number) {
    [this.heap[i], this.heap[j]] = [this.heap[j], this.heap[i]];
  }
  private siftUp(pos: number) {
    while (
      pos > 0 &&
      this.heap[pos].priority > this.heap[this.parent(pos)].priority
    ) {
      this.swap(pos, this.parent(pos));
      pos = this.parent(pos);
    }
  }

  private siftDown(pos: number) {
    let max = pos;
    const l = this.left(pos);
    const r = this.right(pos);

    if (l < this.count && this.heap[l].priority > this.heap[max].priority) {
      max = l;
    }
    if (r < this.count && this.heap[r].priority > this.heap[max].priority) {
      max = r;
    }

    if (max !== pos) {
      this.swap(pos, max);
      this.siftDown(max);
    }
  }

  private grow() {
    const newCapacity = Math.floor(this.heap.length * 1.5);
    const newHeap = new Array(newCapacity);
    for (let i = 0; i < this.heap.length; i++) {
      newHeap[i] = this.heap[i];
    }
    this.heap = newHeap;
  }

  // Memory optimization
  compact() {
    if (this.freeSlots.length === 0) return;
    this.heap = this.heap.slice(0, this.count);
    this.freeSlots = [];
  }

  enqueue(value: Data, priority = 0) {
    let pos;
    if (this.freeSlots.length > 0) {
      pos = this.freeSlots.pop(); // Reuse empty slot
    } else {
      if (this.count === this.heap.length) {
        this.grow(); // Expand array if full
      }
      pos = this.count;
    }

    this.heap[pos] = { value, priority };
    this.siftUp(pos);
    this.count++;
  }

  dequeue() {
    if (this.count === 0) return null;
    const root = this.heap[0];
    this.heap[0] = this.heap[--this.count];
    this.freeSlots.push(this.count); // Mark slot as reusable
    this.siftDown(0);
    return root.value;
  }

  peek() {
    return this.count > 0 ? this.heap[0].value : null;
  }

  isEmpty() {
    return this.count === 0;
  }

  size() {
    return this.count;
  }
}
