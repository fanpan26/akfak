package com.fanpan26.akfak.clients.producer.internals;

import com.fanpan26.akfak.common.errors.TimeoutException;
import com.fanpan26.akfak.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author fanyuepan
 */
public final class BufferPool {

    private final long totalMemory;
    private final int poolableSize;
    private final ReentrantLock lock;
    private final Deque<ByteBuffer> free;
    private final Deque<Condition> waiters;
    private long availableMemory;
    private final Time time;

    public BufferPool(long memory, int poolableSize, Time time) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<ByteBuffer>();
        this.waiters = new ArrayDeque<Condition>();
        this.totalMemory = memory;
        this.availableMemory = memory;
        this.time = time;
    }

    public ByteBuffer allocate(int size,long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory) {
            throw new IllegalArgumentException("Attempt to allocate " + size
                    + " bytes, but there is a hard limit of "
                    + this.totalMemory
                    + " on memory allocations.");
        }
        this.lock.lock();
        try {
            if (size == poolableSize && !this.free.isEmpty()) {
                return this.free.pollFirst();
            }
            int freeListSize = this.free.size() * this.poolableSize;
            if (this.availableMemory + freeListSize >= size) {
                freeUp(size);
                this.availableMemory -= size;
                lock.unlock();
                return ByteBuffer.allocate(size);
            }
            //当前内存不够了，要等待
            int accumulated = 0;
            ByteBuffer buffer = null;
            Condition moreMemory = this.lock.newCondition();
            long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
            this.waiters.addLast(moreMemory);
            while (accumulated < size) {
                long startWaitNs = time.nanoseconds();
                long timeNs;
                boolean waitingTimeElapsed;
                try {
                    waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    this.waiters.remove(moreMemory);
                    throw e;
                } finally {
                    long endWaitMs = time.nanoseconds();
                    timeNs = Math.max(0L, endWaitMs - startWaitNs);
                }
                if (waitingTimeElapsed) {
                    this.waiters.remove(moreMemory);
                    throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                }
                remainingTimeToBlockNs -= timeNs;
                if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                    // just grab a buffer from the free list
                    buffer = this.free.pollFirst();
                    accumulated = size;
                } else {
                    freeUp(size - accumulated);
                    int got = (int) Math.min(size - accumulated, this.availableMemory);
                    this.availableMemory -= got;
                    accumulated += got;
                }
            }
            Condition removed = this.waiters.removeFirst();
            if (removed != moreMemory) {
                throw new IllegalStateException("Wrong condition: this shouldn't happen.");
            }
            if (this.availableMemory > 0 || !this.free.isEmpty()) {
                if (!this.waiters.isEmpty()) {
                    this.waiters.peekFirst().signal();
                }
            }
            lock.unlock();
            if (buffer == null) {
                return ByteBuffer.allocate(size);
            } else {
                return buffer;
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void freeUp(int size) {
        //将队列中的ByteBuffer从队列拉出来，直到空间大小大于等于size
        while (!this.free.isEmpty() && this.availableMemory < size) {
            this.availableMemory += this.free.pollLast().capacity();
        }
    }

    public void deallocate(ByteBuffer buffer,int size){
        lock.lock();
        try{
            //如果释放的大小恰好为poolableSize并且为buffer的总大小，那么直接归还buffer即可
            if (size == this.poolableSize && size == buffer.capacity()){
                buffer.clear();
                this.free.add(buffer);
            }else{
                //否则增加availableMemory的大小
                this.availableMemory += size;
            }
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null) {
                moreMem.signal();
            }
        }
        finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    public long availableMemory() {
        lock.lock();
        try {
            return this.availableMemory + this.free.size() * this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.availableMemory;
        } finally {
            lock.unlock();
        }
    }

    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    Deque<Condition> waiters() {
        return this.waiters;
    }
}

