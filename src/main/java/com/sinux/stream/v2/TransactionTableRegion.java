/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sinux.stream.v2;


import com.sinux.stream.exception.StreamLoadFailException;
import com.sinux.stream.http.StreamLoadEntityMeta;
import com.sinux.stream.loader.Chunk;
import com.sinux.stream.loader.LabelGenerator;
import com.sinux.stream.loader.StreamLoadManager;
import com.sinux.stream.loader.StreamLoadResponse;
import com.sinux.stream.loader.StreamLoadSnapshot;
import com.sinux.stream.loader.StreamLoader;
import com.sinux.stream.loader.TableRegion;
import com.sinux.stream.properties.StreamLoadTableProperties;
import org.apache.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.sinux.stream.exception.ErrorUtils.isRetryable;


public class TransactionTableRegion implements TableRegion {

    enum State {
        ACTIVE,
        FLUSHING,
        COMMITTING
    }
    private static final Logger LOG = LoggerFactory.getLogger(TransactionTableRegion.class);
    private final StreamLoadManager manager;
    private final StreamLoader streamLoader;
    private final LabelGenerator labelGenerator;
    private final String uniqueKey;
    private final String database;
    private final String table;
    private final StreamLoadTableProperties properties;
    private final AtomicLong age = new AtomicLong(0L);
    private final AtomicLong cacheBytes = new AtomicLong();
    private final AtomicLong cacheRows = new AtomicLong();
    private final AtomicReference<State> state;
    private final AtomicBoolean ctl = new AtomicBoolean(false);
    private volatile Chunk activeChunk;
    private final ConcurrentLinkedQueue<Chunk> inactiveChunks = new ConcurrentLinkedQueue<>();
    private volatile String label;
    private volatile Future<?> responseFuture;
    private volatile long lastCommitTimeMills;
    private final int maxRetries;
    private final int retryIntervalInMs;
    private volatile int numRetries;
    private volatile long lastFailTimeMs;

    // First exception if retry many times
    private volatile Throwable firstException;

    public TransactionTableRegion(String uniqueKey,
                            String database,
                            String table,
                            StreamLoadManager manager,
                            StreamLoadTableProperties properties,
                            StreamLoader streamLoader,
                            LabelGenerator labelGenerator,
                            int maxRetries,
                            int retryIntervalInMs) {
        this.uniqueKey = uniqueKey;
        this.database = database;
        this.table = table;
        this.manager = manager;
        this.properties = properties;
        this.streamLoader = streamLoader;
        this.labelGenerator = labelGenerator;
        this.state = new AtomicReference<>(State.ACTIVE);
        this.lastCommitTimeMills = System.currentTimeMillis();
        this.activeChunk = new Chunk(properties.getDataFormat());
        this.maxRetries = maxRetries;
        this.retryIntervalInMs = retryIntervalInMs;
    }

    @Override
    public StreamLoadTableProperties getProperties() {
        return properties;
    }

    @Override
    public String getUniqueKey() {
        return uniqueKey;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public LabelGenerator getLabelGenerator() {
        return labelGenerator;
    }

    @Override
    public void setLabel(String label) {
        // Reuse the same label to avoid duplicate load if retry happens
        if (numRetries > 0 && label != null) {
            return;
        }
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public long getCacheBytes() {
        return cacheBytes.get();
    }

    @Override
    public void resetAge() {
        age.set(0);
    }

    @Override
    public long getAndIncrementAge() {
        return age.getAndIncrement();
    }

    @Override
    public long getAge() {
        return age.get();
    }

    @Override
    public int write(byte[] row) {
        if (row == null) {
            return 0;
        }

        int c;
        if (ctl.compareAndSet(false, true)) {
            c = write0(row);
        } else {
            for (;;) {
                if (ctl.compareAndSet(false, true)) {
                    c = write0(row);
                    break;
                }
            }
        }
        ctl.set(false);
        return c;
    }

    private void switchChunk() {
        if (activeChunk == null || activeChunk.numRows() == 0) {
            return;
        }
        inactiveChunks.add(activeChunk);
        activeChunk = new Chunk(properties.getDataFormat());
    }

    protected int write0(byte[] row) {
        if (activeChunk.estimateChunkSize(row) > properties.getChunkLimit()
                || activeChunk.numRows() >= properties.getMaxBufferRows()) {
            switchChunk();
        }

        activeChunk.addRow(row);
        cacheBytes.addAndGet(row.length);
        cacheRows.incrementAndGet();
        return row.length;
    }

    @Override
    public boolean isFlushing() {
        return state.get() == State.FLUSHING;
    }

    public FlushReason shouldFlush() {
        if (state.get() != State.ACTIVE) {
            return FlushReason.NONE;
        }
        return cacheRows.get() >= properties.getMaxBufferRows() ? FlushReason.BUFFER_ROWS_REACH_LIMIT : FlushReason.NONE;
    }

    public boolean flush(FlushReason reason) {
        if (state.compareAndSet(State.ACTIVE, State.FLUSHING)) {
            for (;;) {
                if (ctl.compareAndSet(false, true)) {
                    LOG.info("Flush uniqueKey : {}, label : {}, bytes : {}, rows: {}, reason: {}",
                            uniqueKey, label, cacheBytes.get(), cacheRows.get(), reason);
                    if (reason != FlushReason.BUFFER_ROWS_REACH_LIMIT ||
                            activeChunk.numRows() >= properties.getMaxBufferRows()) {
                        switchChunk();
                    }
                    ctl.set(false);
                    break;
                }
            }
            if (!inactiveChunks.isEmpty()) {
                streamLoad(0);
                return true;
            } else {
                state.compareAndSet(State.FLUSHING, State.ACTIVE);
                return false;
            }
        }
        return false;
    }

    public boolean commit() {
        if (!state.compareAndSet(State.ACTIVE, State.COMMITTING)) {
            return false;
        }
        boolean commitSuccess;
        if (label != null) {
            StreamLoadSnapshot.Transaction transaction = new StreamLoadSnapshot.Transaction(database, table, label);
            try {
                if (!streamLoader.prepare(transaction)) {
                    String errorMsg = "Failed to prepare transaction, please check taskmanager log for details, " + transaction;
                    throw new StreamLoadFailException(errorMsg);
                }

                if (!streamLoader.commit(transaction)) {
                    String errorMsg = "Failed to commit transaction, please check taskmanager log for details, " + transaction;
                    throw new StreamLoadFailException(errorMsg);
                }
            } catch (Exception e) {
                LOG.error("TransactionTableRegion commit failed, db: {}, table: {}, label: {}", database, table, label, e);
                fail(e);
                return false;
            }

            label = null;
            long commitTime = System.currentTimeMillis();
            long commitDuration = commitTime - lastCommitTimeMills;
            lastCommitTimeMills = commitTime;
            commitSuccess = true;
            LOG.info("Success to commit transaction: {}, duration: {} ms", transaction, commitDuration);
        } else {
            // if the data has never been flushed (label == null), the commit should fail so that StreamLoadManagerV2#init
            // will schedule to flush the data first, and then trigger commit again
            commitSuccess = cacheBytes.get() == 0;
        }
        state.compareAndSet(State.COMMITTING, State.ACTIVE);
        return commitSuccess;
    }
    @Override
    public void fail(Throwable e) {
        if (firstException == null) {
            firstException = e;
        }
        if (numRetries >= maxRetries || !isRetryable(e)) {
            LOG.error("Failed to flush data for db: {}, table: {} after {} times retry, the last exception is",
                    database, table, numRetries, e);
            manager.callback(firstException);
            return;
            //throw  new StreamLoadFailException(e.getMessage());
        }
        responseFuture = null;
        numRetries += 1;
        lastFailTimeMs = System.currentTimeMillis();
        LOG.warn("Failed to flush data for db: {}, table: {}, and will retry for {} times after {} ms",
                database, table, numRetries, retryIntervalInMs, e);
        streamLoad(retryIntervalInMs);
    }
    @Override
    public void complete(StreamLoadResponse response) {
        String tableId=database+"@"+table;
        Chunk chunk = inactiveChunks.remove();
        cacheBytes.addAndGet(-chunk.rowBytes());
        cacheRows.addAndGet(-chunk.numRows());
        response.setFlushBytes(chunk.rowBytes());
        response.setFlushRows(chunk.numRows());
        manager.callback(response,tableId);
        numRetries = 0;
        firstException = null;
        LOG.info("Stream load flushed, db: {}, table: {}, label : {}", database, table, label);
        if (!inactiveChunks.isEmpty()) {
            LOG.info("Stream load continue, db: {}, table: {}, label : {}", database, table, label);
            streamLoad(0);
            return;
        }
        if (state.compareAndSet(State.FLUSHING, State.ACTIVE)) {
            LOG.info("Stream load completed, db: {}, table: {}, label : {}", database, table, label);
        }
    }

    @Override
    public Future<?> getResult() {
        return responseFuture;
    }

    protected void streamLoad(int delayMs) {
        try {
            Chunk chunk = inactiveChunks.peek();
            LOG.info("Stream load chunk, db: {}, table: {}, numRows: {}, rowBytes: {}, chunkBytes: {}",
                    database, table, chunk.numRows(), chunk.rowBytes(), chunk.chunkBytes());
            streamLoader.send(this, delayMs).get();
        } catch (Exception e) {
            fail(e);
        }
    }
    @Override
    public HttpEntity getHttpEntity() {
        return new ChunkHttpEntity(uniqueKey, inactiveChunks.peek());
    }

    @Override
    public long getLastWriteTimeMillis() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setResult(Future<?> result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void callback(StreamLoadResponse response) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFlushBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] read() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamLoadEntityMeta getEntityMeta() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean testPrepare() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean prepare() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean flush() { throw new UnsupportedOperationException(); }

}
