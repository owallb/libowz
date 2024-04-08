// BSD 3-Clause License
//
// Copyright (c) 2024, Oscar Wallberg
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
//    contributors may be used to endorse or promote products derived from
//    this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

const std = @import("std");
const math = std.math;
const mem = std.mem;

const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

const Direction = enum {
    /// Indicates moving forwards, in positive direction.
    Forward,
    /// Indicates moving backwards, in negative direction.
    Backward,
};

pub const GrowError = error{
    /// Unable to allocate more memory.
    OutOfMemory,
    /// The new capacity results in an integer overflow.
    CapacityOverflow,
};

/// A dynamic FIFO queue implemented using a circular buffer.
pub fn DynamicQueue(comptime T: type) type {
    return struct {
        // Allocator for allocating, reallocating and releasing memory.
        allocator: Allocator,
        // Circular buffer that stores the enqueued items.
        // Its length is always a power of 2.
        buf: []T,
        // Front of the queue: index of the next item to be dequeued.
        front: usize = 0,
        // Back of the queue: index where the next item will be enqueued.
        back: usize = 0,
        // Total number of items currently in the queue.
        count: usize = 0,

        const Self = @This();

        /// Creates a new queue.
        pub fn init(allocator: Allocator) Self {
            return Self{
                .allocator = allocator,
                .buf = &[_]T{},
            };
        }

        /// Creates a new queue with an initial capacity, rounded up to the nearest power of 2.
        pub fn initWithCapacity(
            allocator: Allocator,
            capacity: usize,
        ) GrowError!Self {
            var queue = init(allocator);
            try queue.ensureCapacity(capacity);
            return queue;
        }

        /// Releases the buffer and invalidates the queue.
        pub fn deinit(self: *Self) void {
            if (self.buf.len > 0) {
                self.allocator.free(self.buf);
            }
            self.* = undefined;
        }

        /// Enqueues `item` to the back of the queue, growing the buffer if necessary.
        pub fn enqueue(self: *Self, item: T) GrowError!void {
            try self.ensureCapacity(1);
            self.buf[self.back] = item;
            self.addBack(1);
        }

        /// Enqueues multiple items to the back of the queue, growing the buffer if necessary.
        /// The order of the items in `source` is maintained after copying to the queue.
        pub fn enqueueFrom(self: *Self, source: []const T) GrowError!void {
            var num = source.len;
            if (num == 0) {
                return;
            }

            try self.ensureCapacity(source.len);

            while (num > 0) {
                const n = @min(self.buf.len - self.back, num);
                @memcpy(self.buf[self.back..][0..n], source[source.len - num ..][0..n]);
                self.addBack(n);
                num -= n;
            }
        }

        /// Dequeues a single item from the queue and return it.
        /// Returns null if the queue is empty.
        pub fn dequeue(self: *Self) ?T {
            if (self.count == 0) {
                return null;
            }

            const item = self.buf[self.front];
            self.removeFront(1);
            return item;
        }

        /// Dequeues multiple items from the queue into the provided slice.
        /// Returns the number of dequeued items, which is the lesser of `dest.len` and the number
        /// of items in the queue.
        pub fn dequeueInto(self: *Self, dest: []T) usize {
            const num_total: usize = @min(dest.len, self.count);
            var num_left = num_total;

            while (num_left > 0) {
                const n = @min(self.buf.len - self.front, num_left);
                @memcpy(dest[dest.len - num_left ..][0..n], self.buf[self.front..][0..n]);
                self.removeFront(n);
                num_left -= n;
            }

            return num_total;
        }

        /// Copies multiple items from the queue into the provided slice.
        /// Returns the number of copied items, which is the lesser of `dest.len` and the number
        /// of items in the queue.
        pub fn copyInto(self: *const Self, dest: []T) usize {
            const num_total = @min(dest.len, self.count);
            var copied: usize = 0;

            while (copied < num_total) {
                const start = self.getRelativeIndex(self.front, Direction.Forward, copied);
                const n = @min(num_total - copied, self.buf.len - start);

                @memcpy(dest[copied..][0..n], self.buf[start..][0..n]);
                copied += n;
            }

            return copied;
        }

        /// Returns the item at the front of the queue without removing it, or null if empty.
        pub fn peek(self: *const Self) ?T {
            if (self.count == 0) {
                return null;
            }

            return self.buf[self.front];
        }

        /// Discards up to `count` number of items from the front of the queue.
        /// Returns the number of discarded items, which is the lesser of `count` and the number of
        /// items in the queue.
        pub fn discard(self: *Self, count: usize) usize {
            const num: usize = @min(count, self.count);

            if (num == 0) {
                return num;
            }

            self.removeFront(num);
            return num;
        }

        /// Advances the front position `count` steps forward, wrapping around if necessary.
        /// Decrements the number of items in the queue by `count`.
        /// Asserts `self.count >= count`.
        fn removeFront(self: *Self, count: usize) void {
            assert(self.count >= count);
            self.front = self.getRelativeIndex(self.front, .Forward, count);
            self.count -= count;
        }

        /// Advances the back position `count` steps forwad, wrapping around if necessary.
        /// Increments the number of items in the queue by `count`.
        /// Asserts `self.buf.len >= (self.count + count)`
        fn addBack(self: *Self, count: usize) void {
            assert(self.buf.len >= (self.count + count));
            self.back = self.getRelativeIndex(self.back, .Forward, count);
            self.count += count;
        }

        /// Calculates a shifted position of `index` by `count` steps in `direction`.
        /// Asserts `self.buf.len` is a power of 2.
        fn getRelativeIndex(
            self: *const Self,
            index: usize,
            direction: Direction,
            count: usize,
        ) usize {
            assert(math.isPowerOfTwo(self.buf.len));
            const len = self.buf.len;
            const c: usize = switch (direction) {
                .Forward => count,
                .Backward => len -% count,
            };

            return (index +% c) & (len - 1);
        }

        /// Ensures the queue has capacity for `count` additional items, growing if necessary.
        fn ensureCapacity(self: *Self, count: usize) GrowError!void {
            if (self.buf.len -| self.count >= count) {
                return;
            }

            const capacity = math.add(usize, self.count, count) catch |err| switch (err) {
                error.Overflow => return GrowError.CapacityOverflow,
            };

            const old_capacity = self.buf.len;
            const new_capacity = try getAlignedCapacity(capacity);

            if (old_capacity == 0) {
                self.buf = try self.allocator.alloc(T, new_capacity);

                return;
            }

            self.buf = try self.allocator.realloc(self.buf, new_capacity);

            const last_idx = self.getRelativeIndex(self.back, .Backward, 1);
            if (last_idx < self.front) {
                @memcpy(self.buf[old_capacity..][0..self.back], self.buf[0..self.back]);
            }

            self.back = self.getRelativeIndex(self.front, Direction.Forward, self.count);
        }

        /// Calculates a capacity aligned to the next power of 2, unless it already is one.
        fn getAlignedCapacity(capacity: usize) error{CapacityOverflow}!usize {
            if (capacity == 0) {
                return 1;
            } else if (math.isPowerOfTwo(capacity)) {
                return capacity;
            } else {
                return math.ceilPowerOfTwo(usize, capacity) catch |err| switch (err) {
                    error.Overflow => error.CapacityOverflow,
                };
            }
        }
    };
}

const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualDeep = std.testing.expectEqualDeep;
const expectEqualSlices = std.testing.expectEqualSlices;
const expectError = std.testing.expectError;

test "enqueue and dequeue" {
    var queue = DynamicQueue(i32).init(std.testing.allocator);
    defer queue.deinit();
    var tmp = [_]i32{undefined} ** 3;

    try expectEqual(0, queue.buf.len);

    {
        try queue.enqueue(11);
        try expectEqual(1, queue.buf.len);
        try expectEqual(1, queue.count);
        try expectEqual(11, queue.buf[queue.front]);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{11}, tmp[0..copied]);
    }

    {
        try queue.enqueue(22);
        try expectEqual(2, queue.count);
        try expectEqual(2, queue.buf.len);
        try expectEqual(11, queue.buf[queue.front]);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{ 11, 22 }, tmp[0..copied]);
    }

    {
        try queue.enqueue(33);
        try expectEqual(3, queue.count);
        try expectEqual(4, queue.buf.len);
        try expectEqual(11, queue.buf[queue.front]);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{ 11, 22, 33 }, tmp[0..copied]);
    }

    {
        const item = queue.dequeue().?;
        try expectEqual(11, item);
        try expectEqual(2, queue.count);
        try expectEqual(22, queue.buf[queue.front]);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{ 22, 33 }, tmp[0..copied]);
    }

    {
        const item = queue.dequeue().?;
        try expectEqual(22, item);
        try expectEqual(1, queue.count);
        try expectEqual(33, queue.buf[queue.front]);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{33}, tmp[0..copied]);
    }

    {
        const item = queue.dequeue().?;
        try expectEqual(33, item);
        try expectEqual(0, queue.count);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{}, tmp[0..copied]);
    }
}

test "enqueueFrom and dequeueInto" {
    var queue = DynamicQueue(i32).init(std.testing.allocator);
    defer queue.deinit();

    var tmp = [_]i32{undefined} ** 3;

    try expectEqual(0, queue.buf.len);

    {
        const source = [_]i32{ 11, 22, 33 };
        try queue.enqueueFrom(&source);
        try expectEqual(4, queue.buf.len);
        try expectEqual(3, queue.count);
        try expectEqual(11, queue.buf[queue.front]);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &source, tmp[0..copied]);
    }

    {
        var dest = [_]i32{undefined} ** 2;
        const count = queue.dequeueInto(&dest);
        try expectEqual(2, count);
        try expectEqual(1, queue.count);
        try expectEqual(33, queue.buf[queue.front]);
        try expectEqualSlices(i32, &[_]i32{ 11, 22 }, &dest);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{33}, tmp[0..copied]);
    }
}

test "peek" {
    var queue = DynamicQueue(i32).init(std.testing.allocator);
    defer queue.deinit();

    var tmp = [_]i32{undefined} ** 3;

    try queue.enqueue(42);
    try queue.enqueue(43);

    {
        const peeked_item = queue.peek().?;
        try expectEqual(42, peeked_item);
        try expectEqual(2, queue.count);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{ 42, 43 }, tmp[0..copied]);
    }

    {
        const discarded = queue.discard(1);
        try expectEqual(1, discarded);
        const peeked_item = queue.peek().?;
        try expectEqual(43, peeked_item);
        try expectEqual(1, queue.count);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{43}, tmp[0..copied]);
    }
}

test "copyInto" {
    var queue = DynamicQueue(i32).init(std.testing.allocator);
    defer queue.deinit();

    var tmp = [_]i32{undefined} ** 4;
    const data = [_]i32{ 11, 22, 33, 44 };

    {
        try queue.enqueueFrom(&data);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &data, tmp[0..copied]);
    }

    {
        const discarded = queue.discard(2);
        try expectEqual(2, discarded);
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, data[2..], tmp[0..copied]);
    }
}

test "initWithCapacity" {
    const capacities = [_]usize{ 0, 1, 2, 3, 4, 5, 31, 32 };
    const expected = [_]usize{ 0, 1, 2, 4, 4, 8, 32, 32 };

    for (capacities, expected) |cap, exp| {
        var queue = try DynamicQueue(i32).initWithCapacity(std.testing.allocator, cap);
        defer queue.deinit();
        const actual = queue.buf.len;
        try std.testing.expectEqual(exp, actual);
    }
}

test "discard" {
    var queue = DynamicQueue(i32).init(std.testing.allocator);
    defer queue.deinit();

    const source = [_]i32{ 1, 2, 3, 4, 5 };
    try queue.enqueueFrom(&source);
    const discarded = queue.discard(2);
    try expectEqual(2, discarded);
    try expectEqual(3, queue.count);
    var tmp = [_]i32{undefined} ** 5;
    const copied = queue.copyInto(&tmp);
    try expectEqualSlices(i32, &[_]i32{ 3, 4, 5 }, tmp[0..copied]);
}

test "empty queue" {
    var queue = DynamicQueue(i32).init(std.testing.allocator);
    defer queue.deinit();

    const item = queue.dequeue();
    try expectEqual(null, item);

    var dest = [_]i32{undefined} ** 1;
    const count = queue.dequeueInto(&dest);
    try expectEqual(0, count);

    const peeked_item = queue.peek();
    try expect(peeked_item == null);

    const discarded = queue.discard(1);
    try expectEqual(0, discarded);
}

test "buffer wrapping" {
    var queue = try DynamicQueue(i32).initWithCapacity(std.testing.allocator, 4);
    defer queue.deinit();

    try expectEqual(4, queue.buf.len);

    {
        const source = [_]i32{ 1, 2, 3, 4 };
        try queue.enqueueFrom(&source);
        try expectEqual(4, queue.buf.len);
        try expectEqual(4, queue.count);
        var tmp = [_]i32{undefined} ** 8;
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &source, tmp[0..copied]);
    }

    {
        var dest = [_]i32{undefined} ** 2;
        const count = queue.dequeueInto(&dest);
        try expectEqual(2, count);
        try expectEqual(2, queue.count);
        try expectEqualSlices(i32, &[_]i32{ 1, 2 }, &dest);
        var tmp = [_]i32{undefined} ** 8;
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{ 3, 4 }, tmp[0..copied]);
    }

    {
        const source = [_]i32{ 5, 6, 7, 8 };
        try queue.enqueueFrom(&source);
        try expectEqual(8, queue.buf.len);
        try expectEqual(6, queue.count);
        var tmp = [_]i32{undefined} ** 8;
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{ 3, 4, 5, 6, 7, 8 }, tmp[0..copied]);
    }

    {
        var dest = [_]i32{undefined} ** 4;
        const count = queue.dequeueInto(&dest);
        try expectEqual(4, count);
        try expectEqual(2, queue.count);
        try expectEqualSlices(i32, &[_]i32{ 3, 4, 5, 6 }, &dest);
        var tmp = [_]i32{undefined} ** 8;
        const copied = queue.copyInto(&tmp);
        try expectEqualSlices(i32, &[_]i32{ 7, 8 }, tmp[0..copied]);
    }
}

test "CapacityOverflow" {
    var queue = DynamicQueue(u8).init(std.testing.allocator);
    defer queue.deinit();

    const max_capacity = 1 << (@typeInfo(usize).Int.bits - 1);
    try expectError(GrowError.CapacityOverflow, DynamicQueue(u8).getAlignedCapacity(max_capacity + 1));

    queue.count = math.maxInt(usize);
    try expectError(GrowError.CapacityOverflow, queue.ensureCapacity(1));
}

test "OutOfMemory" {
    var queue = DynamicQueue(i1).init(std.testing.allocator);
    defer queue.deinit();

    const max_capacity = 1 << (@typeInfo(usize).Int.bits - 1);
    try expectError(GrowError.OutOfMemory, queue.ensureCapacity(max_capacity));
}
