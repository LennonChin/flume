/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.file;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.flume.channel.file.encryption.KeyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Processes a set of data logs, replaying said logs into the queue.
 */
class ReplayHandler {
  private static final Logger LOG = LoggerFactory
      .getLogger(ReplayHandler.class);
  private final FlumeEventQueue queue;
  private final long lastCheckpoint;
  private final Map<Integer, LogFile.SequentialReader> readers;
  private final PriorityQueue<LogRecord> logRecordBuffer;
  private final KeyProvider encryptionKeyProvider;
  private final boolean fsyncPerTransaction;
  /**
   * This data structure stores takes for which we found a commit in the log
   * files before we found a commit for the put. This can happen if the channel
   * is configured for multiple directories.
   *
   * Consider the following:
   *
   * logdir1, logdir2
   *
   * Put goes to logdir2 Commit of Put goes to logdir2 Take goes to logdir1
   * Commit of Take goes to logdir1
   *
   * When replaying we will start with log1 and find the take and commit before
   * finding the put and commit in logdir2.
   */
  private final List<Long> pendingTakes;
  int readCount = 0;
  int putCount = 0;
  int takeCount = 0;
  int rollbackCount = 0;
  int commitCount = 0;
  int skipCount = 0;

  @VisibleForTesting
  public int getReadCount() {
    return readCount;
  }
  @VisibleForTesting
  public int getPutCount() {
    return putCount;
  }
  @VisibleForTesting
  public int getTakeCount() {
    return takeCount;
  }
  @VisibleForTesting
  public int getCommitCount() {
    return commitCount;
  }

  @VisibleForTesting
  public int getRollbackCount() {
    return rollbackCount;
  }

  ReplayHandler(FlumeEventQueue queue,
                @Nullable KeyProvider encryptionKeyProvider,
                boolean fsyncPerTransaction) {
    this.queue = queue;
    this.lastCheckpoint = queue.getLogWriteOrderID();
    pendingTakes = Lists.newArrayList();
    readers = Maps.newHashMap();
    logRecordBuffer = new PriorityQueue<LogRecord>();
    this.encryptionKeyProvider = encryptionKeyProvider;
    this.fsyncPerTransaction = fsyncPerTransaction;
  }

  /**
   * Replay logic from Flume1.2 which can be activated if the v2 logic
   * is failing on ol logs for some reason.
   *
   * V1版本重放
   */
  @Deprecated
  void replayLogv1(List<File> logs) throws Exception {
    int total = 0;
    int count = 0;
    MultiMap transactionMap = new MultiValueMap();
    //Read inflight puts to see if they were committed
    // 读取正在处理的PUT，看它们是否被COMMIT了
    // 反序列化正在处理的PUT，然后遍历
    SetMultimap<Long, Long> inflightPuts = queue.deserializeInflightPuts();
    for (Long txnID : inflightPuts.keySet()) {
      // 根据事务ID获取事件指针
      Set<Long> eventPointers = inflightPuts.get(txnID);
      // 将事件指针存入transactionMap
      for (Long eventPointer : eventPointers) {
        // 键是事务ID，值是FileID和offset组成的FlumeEventPointer
        transactionMap.put(txnID, FlumeEventPointer.fromLong(eventPointer));
      }
    }

    // 读取并反序列化正在处理的TAKE
    SetMultimap<Long, Long> inflightTakes = queue.deserializeInflightTakes();

    LOG.info("Starting replay of " + logs);
    // 遍历所有的log文件，合并Checkpoint种的PUT和TAKE
    for (File log : logs) {
      LOG.info("Replaying " + log);
      LogFile.SequentialReader reader = null;
      try {
        // 获取log文件的顺序读写器
        reader = LogFileFactory.getSequentialReader(log,
          encryptionKeyProvider, fsyncPerTransaction);
        // Skip到checkpoint记录的最新Position
        reader.skipToLastCheckpointPosition(queue.getLogWriteOrderID());
        LogRecord entry;
        FlumeEventPointer ptr;
        // for puts the fileId is the fileID of the file they exist in
        // for takes the fileId and offset are pointers to a put
        // 获取File ID
        int fileId = reader.getLogFileID();

        while ((entry = reader.next()) != null) {
          // 获取偏移量
          int offset = entry.getOffset();
          // 获取事务记录
          TransactionEventRecord record = entry.getEvent();
          // 获取事务类型
          short type = record.getRecordType();
          // 获取事务ID
          long trans = record.getTransactionID();
          readCount++;

          /**
           * 以Checkpoint记录的事务为基础，合并log日志记录的事务，分两种情况：
           * 1. log日志中写顺序ID > Checkpoint最后记录的写顺序ID，
           *    说明log日志中的事务是新事务，则需要合并。
           * 2. log日志中写顺序ID <= Checkpoint最后记录的写顺序ID，
           *    说明log日志中的事务在Checkpoint中已经记录了，不需要合并，直接跳过即可。
           */
          if (record.getLogWriteOrderID() > lastCheckpoint) {
            // log日志文件记录的事务的写顺序ID > Checkpoint记录的写顺序ID，需要合并。
            if (type == TransactionEventRecord.Type.PUT.get()) { // PUT
              putCount++;
              ptr = new FlumeEventPointer(fileId, offset);
              // 添加到transactionMap
              transactionMap.put(trans, ptr);
            } else if (type == TransactionEventRecord.Type.TAKE.get()) { // TAKE
              takeCount++;
              Take take = (Take) record;
              ptr = new FlumeEventPointer(take.getFileID(), take.getOffset());
              // 添加到transactionMap
              transactionMap.put(trans, ptr);
            } else if (type == TransactionEventRecord.Type.ROLLBACK.get()) { // ROLLBACK
              rollbackCount++;
              // 从transactionMap移除对应的事务
              transactionMap.remove(trans);
            } else if (type == TransactionEventRecord.Type.COMMIT.get()) { // COMMIT
              /**
               * 此时需要处理transactionMap和inflighttakes中的TAKE
               * 对于transactionMap和inflighttakes中的TAKE操作，如果发现有COMMIT，就进行合并后提交
               */
              commitCount++;
              @SuppressWarnings("unchecked")
              // 先移除transactionMap中当前COMMIT对应的已有的TAKE，放入一个集合
              Collection<FlumeEventPointer> pointers =
                  (Collection<FlumeEventPointer>) transactionMap.remove(trans);
              // 处理TAKE的COMMIT，将inflightTakes中当前COMMIT对应的TAKE和pointers集合合并到一起
              if (((Commit) record).getType() == TransactionEventRecord.Type.TAKE.get()) {
                if (inflightTakes.containsKey(trans)) { // COMMIT TAKE
                  if (pointers == null) {
                    pointers = Sets.newHashSet();
                  }
                  // 移除并遍历inflightTakes中对应的TAKE
                  Set<Long> takes = inflightTakes.removeAll(trans);
                  Iterator<Long> it = takes.iterator();
                  while (it.hasNext()) {
                    Long take = it.next();
                    // 记录到pointers中
                    pointers.add(FlumeEventPointer.fromLong(take));
                  }
                }
              }
              if (pointers != null && pointers.size() > 0) {
                // 对合并后的PUT及TAKE进行统一COMMIT
                processCommit(((Commit) record).getType(), pointers);
                count += pointers.size();
              }
            } else {
              Preconditions.checkArgument(false,
                                          "Unknown record type: " + Integer.toHexString(type));
            }

          } else {
            // log日志中写顺序ID <= Checkpoint最后记录的写顺序ID，说明log日志中的事务在Checkpoint中已经记录了，不需要合并，直接跳过即可。
            skipCount++;
          }
        }
        LOG.info("Replayed " + count + " from " + log);
        if (LOG.isDebugEnabled()) {
          LOG.debug("read: " + readCount + ", put: " + putCount + ", take: "
              + takeCount + ", rollback: " + rollbackCount + ", commit: "
              + commitCount + ", skipp: " + skipCount);
        }
      } catch (EOFException e) {
        LOG.warn("Hit EOF on " + log);
      } finally {
        total += count;
        count = 0;
        if (reader != null) {
          reader.close();
        }
      }
    }
    //re-insert the events in the take map,
    //since the takes were not committed.
    int uncommittedTakes = 0;
    // 将inflighttakes未提交的TAKE提取出来，放入到queue头部
    for (Long inflightTxnId : inflightTakes.keySet()) {
      Set<Long> inflightUncommittedTakes =
              inflightTakes.get(inflightTxnId);
      for (Long inflightUncommittedTake : inflightUncommittedTakes) {
        queue.addHead(FlumeEventPointer.fromLong(inflightUncommittedTake));
        uncommittedTakes++;
      }
    }
    inflightTakes.clear();
    count += uncommittedTakes;

    // 等待处理的TAKE
    int pendingTakesSize = pendingTakes.size();
    if (pendingTakesSize > 0) {
      String msg = "Pending takes " + pendingTakesSize
          + " exist after the end of replay";
      if (LOG.isDebugEnabled()) {
        for (Long pointer : pendingTakes) {
          LOG.debug("Pending take " + FlumeEventPointer.fromLong(pointer));
        }
      } else {
        LOG.error(msg + ". Duplicate messages will exist in destination.");
      }
    }
    LOG.info("Replayed " + total);
  }
  /**
   * Replay logs in order records were written
   *
   * V2版本重放
   *
   * @param logs
   * @throws IOException
   */
  void replayLog(List<File> logs) throws Exception {
    int count = 0;
    MultiMap transactionMap = new MultiValueMap();
    // seed both with the highest known sequence of either the tnxid or woid
    long transactionIDSeed = lastCheckpoint, writeOrderIDSeed = lastCheckpoint;
    LOG.info("Starting replay of " + logs);
    //Load the inflight puts into the transaction map to see if they were
    //committed in one of the logs.
    // 反序列化并加载inflightputs到transactionMap
    SetMultimap<Long, Long> inflightPuts = queue.deserializeInflightPuts();
    for (Long txnID : inflightPuts.keySet()) {
      Set<Long> eventPointers = inflightPuts.get(txnID);
      for (Long eventPointer : eventPointers) {
        transactionMap.put(txnID, FlumeEventPointer.fromLong(eventPointer));
      }
    }

    // 反序列化inflighttakes
    SetMultimap<Long, Long> inflightTakes = queue.deserializeInflightTakes();

    // 处理log日志文件
    try {
      // 遍历log日志文件
      for (File log : logs) {
        LOG.info("Replaying " + log);
        try {
          // 获取log文件的顺序读写器
          LogFile.SequentialReader reader =
              LogFileFactory.getSequentialReader(log, encryptionKeyProvider, fsyncPerTransaction);
          // Skip到checkpoint记录的最新Position
          reader.skipToLastCheckpointPosition(queue.getLogWriteOrderID());
          Preconditions.checkState(!readers.containsKey(reader.getLogFileID()),
              "Readers " + readers + " already contains "
                  + reader.getLogFileID());

          // 记录File ID和对应的顺序读写器之间的映射关系
          readers.put(reader.getLogFileID(), reader);
          // 获取log文件中下一条日志记录
          LogRecord logRecord = reader.next();
          if (logRecord == null) { // 如果获取为空，说明当前log文件处理完成了
            // 将对应的顺序读取器移除并关闭
            readers.remove(reader.getLogFileID());
            reader.close();
          } else {
            // 否则将读取到的日志记录加入到logRecordBuffer（PriorityQueue优先队列）
            logRecordBuffer.add(logRecord);
          }
        } catch (EOFException e) {
          LOG.warn("Ignoring " + log + " due to EOF", e);
        }
      }

      // 至此，log文件里的数据都读到logRecordBuffer里了
      LogRecord entry = null;
      FlumeEventPointer ptr = null;
      // 从logRecordBuffer里取数据
      while ((entry = next()) != null) {
        // for puts the fileId is the fileID of the file they exist in
        // for takes the fileId and offset are pointers to a put
        // 获取File ID
        int fileId = entry.getFileID();
        // 获取偏移量
        int offset = entry.getOffset();
        TransactionEventRecord record = entry.getEvent();
        // 获取记录类型
        short type = record.getRecordType();
        // 获取事务ID
        long trans = record.getTransactionID();
        transactionIDSeed = Math.max(transactionIDSeed, trans);
        writeOrderIDSeed = Math.max(writeOrderIDSeed,
            record.getLogWriteOrderID());
        readCount++;
        // 每1000条记录打印一次日志
        if (readCount % 10000 == 0 && readCount > 0) {
          LOG.info("read: " + readCount + ", put: " + putCount + ", take: "
              + takeCount + ", rollback: " + rollbackCount + ", commit: "
              + commitCount + ", skip: " + skipCount + ", eventCount:" + count);
        }

        /**
         * 与V1版本类似，以Checkpoint记录的事务为基础，合并遍历的log日志记录的事务，分两种情况：
         * 1. 遍历的log日志中写顺序ID > Checkpoint最后记录的写顺序ID，
         *    说明log日志中的事务是新事务，则需要合并。
         * 2. 遍历的log日志中写顺序ID <= Checkpoint最后记录的写顺序ID，
         *    说明log日志中的事务在Checkpoint中已经记录了，不需要合并，直接跳过即可。
         */
        if (record.getLogWriteOrderID() > lastCheckpoint) {
          // log日志文件记录的事务的写顺序ID > Checkpoint记录的写顺序ID，需要合并。
          if (type == TransactionEventRecord.Type.PUT.get()) { // PUT
            putCount++;
            ptr = new FlumeEventPointer(fileId, offset);
            // 加入transactionMap
            transactionMap.put(trans, ptr);
          } else if (type == TransactionEventRecord.Type.TAKE.get()) { // TAKE
            takeCount++;
            Take take = (Take) record;
            ptr = new FlumeEventPointer(take.getFileID(), take.getOffset());
            // 加入transactionMap
            transactionMap.put(trans, ptr);
          } else if (type == TransactionEventRecord.Type.ROLLBACK.get()) { // ROLLBACK
            rollbackCount++;
            // 从transactionMap移除对应的PUT或TAKE事务
            transactionMap.remove(trans);
          } else if (type == TransactionEventRecord.Type.COMMIT.get()) { // COMMIT
            commitCount++;
            /**
             * 此时需要处理transactionMap和inflighttakes中的TAKE
             * 对于transactionMap和inflighttakes中的TAKE操作，如果发现有COMMIT，就进行合并后提交
             */
            @SuppressWarnings("unchecked")
            // 先移除transactionMap中当前COMMIT对应的已有的TAKE和PUT，放入一个集合
            Collection<FlumeEventPointer> pointers =
                (Collection<FlumeEventPointer>) transactionMap.remove(trans);
            // 处理TAKE的COMMIT，将inflightTakes中当前COMMIT对应的TAKE和pointers集合合并到一起
            if (((Commit) record).getType()
                    == TransactionEventRecord.Type.TAKE.get()) { // COMMIT TAKE
              if (inflightTakes.containsKey(trans)) {
                if (pointers == null) {
                  pointers = Sets.newHashSet();
                }
                // 移除并遍历inflightTakes中对应的TAKE
                Set<Long> takes = inflightTakes.removeAll(trans);
                Iterator<Long> it = takes.iterator();
                while (it.hasNext()) {
                  Long take = it.next();
                  // 记录到pointers中
                  pointers.add(FlumeEventPointer.fromLong(take));
                }
              }
            }
            if (pointers != null && pointers.size() > 0) {
              // 对合并后的PUT及TAKE进行统一COMMIT
              processCommit(((Commit) record).getType(), pointers);
              count += pointers.size();
            }
          } else {
            Preconditions.checkArgument(false, "Unknown record type: "
                + Integer.toHexString(type));
          }
        } else {
          // log日志中写顺序ID <= Checkpoint最后记录的写顺序ID，说明遍历到的log日志中的事务在Checkpoint中已经记录了，不需要合并，直接跳过即可。
          skipCount++;
        }
      }
      LOG.info("read: " + readCount + ", put: " + putCount + ", take: "
          + takeCount + ", rollback: " + rollbackCount + ", commit: "
          + commitCount + ", skip: " + skipCount + ", eventCount:" + count);
      queue.replayComplete();
    } finally {
      TransactionIDOracle.setSeed(transactionIDSeed);
      WriteOrderOracle.setSeed(writeOrderIDSeed);
      for (LogFile.SequentialReader reader : readers.values()) {
        if (reader != null) {
          reader.close();
        }
      }
    }
    //re-insert the events in the take map,
    //since the takes were not committed.
    int uncommittedTakes = 0;
    // 将inflighttakes未提交的TAKE提取出来，放入到queue头部
    for (Long inflightTxnId : inflightTakes.keySet()) {
      Set<Long> inflightUncommittedTakes =
              inflightTakes.get(inflightTxnId);
      for (Long inflightUncommittedTake : inflightUncommittedTakes) {
        queue.addHead(FlumeEventPointer.fromLong(inflightUncommittedTake));
        uncommittedTakes++;
      }
    }
    inflightTakes.clear();
    count += uncommittedTakes;

    // 等待处理的TAKE
    int pendingTakesSize = pendingTakes.size();
    if (pendingTakesSize > 0) {
      LOG.info("Pending takes " + pendingTakesSize + " exist after the" +
          " end of replay. Duplicate messages will exist in" +
          " destination.");
    }
  }
  private LogRecord next() throws IOException, CorruptEventException {
    LogRecord resultLogRecord = logRecordBuffer.poll();
    if (resultLogRecord != null) {
      // there is more log records to read
      LogFile.SequentialReader reader = readers.get(resultLogRecord.getFileID());
      LogRecord nextLogRecord;
      if ((nextLogRecord = reader.next()) != null) {
        logRecordBuffer.add(nextLogRecord);
      }
    }
    return resultLogRecord;
  }

  // 处理COMMIT
  private void processCommit(short type, Collection<FlumeEventPointer> pointers) {
    if (type == TransactionEventRecord.Type.PUT.get()) {
      for (FlumeEventPointer pointer : pointers) {
        if (!queue.addTail(pointer)) {
          throw new IllegalStateException("Unable to add "
              + pointer + ". Queue depth = " + queue.getSize()
              + ", Capacity = " + queue.getCapacity());
        }
        if (pendingTakes.remove(pointer.toLong())) {
          Preconditions.checkState(queue.remove(pointer),
              "Take was pending and pointer was successfully added to the"
                  + " queue but could not be removed: " + pointer);
        }
      }
    } else if (type == TransactionEventRecord.Type.TAKE.get()) {
      for (FlumeEventPointer pointer : pointers) {
        boolean removed = queue.remove(pointer);
        if (!removed) {
          pendingTakes.add(pointer.toLong());
        }
      }
    } else {
      Preconditions.checkArgument(false,
          "Unknown record type: " + Integer.toHexString(type));
    }
  }

}
