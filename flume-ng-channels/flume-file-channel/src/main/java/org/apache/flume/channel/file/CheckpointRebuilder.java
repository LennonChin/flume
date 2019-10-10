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

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class CheckpointRebuilder {

  private final List<File> logFiles;
  private final FlumeEventQueue queue;
  private final Set<ComparableFlumeEventPointer> committedPuts = Sets.newHashSet();
  private final Set<ComparableFlumeEventPointer> pendingTakes = Sets.newHashSet();
  private final SetMultimap<Long, ComparableFlumeEventPointer> uncommittedPuts =
      HashMultimap.create();
  private final SetMultimap<Long, ComparableFlumeEventPointer>
      uncommittedTakes = HashMultimap.create();
  private final boolean fsyncPerTransaction;

  private static Logger LOG = LoggerFactory.getLogger(CheckpointRebuilder.class);

  public CheckpointRebuilder(List<File> logFiles, FlumeEventQueue queue,
                             boolean fsyncPerTransaction) throws IOException {
    this.logFiles = logFiles;
    this.queue = queue;
    this.fsyncPerTransaction = fsyncPerTransaction;
  }

  // 快速重放
  public boolean rebuild() throws IOException, Exception {
    LOG.info("Attempting to fast replay the log files.");
    List<LogFile.SequentialReader> logReaders = Lists.newArrayList();

    // 遍历dataDir下的log文件
    for (File logFile : logFiles) {
      try {
        // 获取log文件的有序读取器
        logReaders.add(LogFileFactory.getSequentialReader(logFile, null,
            fsyncPerTransaction));
      } catch (EOFException e) {
        LOG.warn("Ignoring " + logFile + " due to EOF", e);
      }
    }
    long transactionIDSeed = 0;
    long writeOrderIDSeed = 0;
    try {

      // 遍历所有log文件的有序读取器
      for (LogFile.SequentialReader log : logReaders) {
        LogRecord entry;
        // 获取文件ID
        int fileID = log.getLogFileID();
        // 循环获取下一个条目
        while ((entry = log.next()) != null) {
          // 偏移量
          int offset = entry.getOffset();
          // 获取事件
          TransactionEventRecord record = entry.getEvent();
          // 事务ID
          long trans = record.getTransactionID();
          // 写顺序ID
          long writeOrderID = record.getLogWriteOrderID();
          // 得到较大的事务ID
          transactionIDSeed = Math.max(trans, transactionIDSeed);
          // 得到较大的写顺序ID
          writeOrderIDSeed = Math.max(writeOrderID, writeOrderIDSeed);

          /**
           * 判断日志的类型是PUT、TAKE、COMMIT还是ROLLBACK，这里的三类操作都是针对每条日志。
           * 1. 日志的PUT和TAKE都必须在COMMIT之后才算执行成功。
           * 2. 对同一条日志的TAKE必须在PUT且COMMIT之后才允许。
           * 3. 在一条日志的PUT还未COMMIT之前，是不能对其进行TAKE的。
           * 4. 为COMMIT的PUT或TAKE是可以通过ROLLBACK回滚的。
           * 5. ROLLBACK只是将uncommittedPuts和uncommittedTakes中未提交的PUT和TAKE删除即可。
           * 6. 最终committedPuts中已提交但还未被TAKE的PUT操作会单独放入queue队列，等待后续处理。
           */
          if (record.getRecordType() == TransactionEventRecord.Type.PUT.get()) { // PUT
            // 记录到uncommittedPuts中
            uncommittedPuts.put(record.getTransactionID(),
                new ComparableFlumeEventPointer(
                    new FlumeEventPointer(fileID, offset),
                    record.getLogWriteOrderID()));
          } else if (record.getRecordType() == TransactionEventRecord.Type.TAKE.get()) { // TAKE
            Take take = (Take) record;
            // 记录到uncommittedTakes中
            uncommittedTakes.put(record.getTransactionID(),
                new ComparableFlumeEventPointer(
                    new FlumeEventPointer(take.getFileID(), take.getOffset()),
                    record.getLogWriteOrderID()));
          } else if (record.getRecordType() == TransactionEventRecord.Type.COMMIT.get()) { // COMMIT
            Commit commit = (Commit) record;

            // 该COMMIT对应的是PUT操作
            if (commit.getType() == TransactionEventRecord.Type.PUT.get()) {
              // 获取该COMMIT对应的所有PUT
              Set<ComparableFlumeEventPointer> puts =
                  uncommittedPuts.get(record.getTransactionID());
              if (puts != null) {
                for (ComparableFlumeEventPointer put : puts) {
                  /**
                   * 如果pendingTasks中包含put，则将其移除，不做处理；
                   * 但如果pendingTasks中不包含put，说明不是还在等待处理的put，
                   * 那么本次的commit则可以直接提交该put，
                   * 因此将该put添加到committedPuts中，表示该put已提交。
                   */
                  if (!pendingTakes.remove(put)) {
                    // 记录到committedPuts中
                    committedPuts.add(put);
                  }
                }
              }
            } else {

              // 该COMMIT对应的是TAKE操作
              // 获取该COMMIT对应的所有TAKE
              Set<ComparableFlumeEventPointer> takes =
                  uncommittedTakes.get(record.getTransactionID());
              if (takes != null) {
                for (ComparableFlumeEventPointer take : takes) {
                  /**
                   * 需要注意：take必须在put被commit之后。
                   * 如果committedPuts中包含该take，表示该take操作的日志的put已经commit了，
                   * 因为对应的put已经commit了，当然可以进行take，将其移除，表示准备提交该take。
                   * 但如果committedPuts中不包含take操作的日志的put，
                   * 说明该日志的put还未提交，因此不能take，还需要等待，
                   * 此时会将该take添加到pendingTakes中，表示take在等待处理。
                   */
                  if (!committedPuts.remove(take)) {
                    // 记录到pendingTakes中
                    pendingTakes.add(take);
                  }
                }
              }
            }
          } else if (record.getRecordType() == TransactionEventRecord.Type.ROLLBACK.get()) { // ROLLBACK
            // 回滚操作
            if (uncommittedPuts.containsKey(record.getTransactionID())) {
              // 回滚没有commit的put，直接将对应的所有put从uncommittedPuts中移除即可。
              uncommittedPuts.removeAll(record.getTransactionID());
            } else {
              // 回滚没有commit的take，直接将对应的所有take从uncommittedTakes中移除即可。
              uncommittedTakes.removeAll(record.getTransactionID());
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Error while generating checkpoint using fast generation logic", e);
      return false;
    } finally {
      TransactionIDOracle.setSeed(transactionIDSeed);
      WriteOrderOracle.setSeed(writeOrderIDSeed);
      // 关闭所有日志文件的顺序读取器
      for (LogFile.SequentialReader reader : logReaders) {
        reader.close();
      }
    }

    // 对剩余的已提交的put构造有序树集合
    Set<ComparableFlumeEventPointer> sortedPuts = Sets.newTreeSet(committedPuts);
    int count = 0;
    // 遍历有序的put，添加到FlumeEventQueue队列中
    for (ComparableFlumeEventPointer put : sortedPuts) {
      queue.addTail(put.pointer);
      count++;
    }
    LOG.info("Replayed {} events using fast replay logic.", count);
    return true;
  }

  private void writeCheckpoint() throws IOException {
    long checkpointLogOrderID = 0;
    List<LogFile.MetaDataWriter> metaDataWriters = Lists.newArrayList();
    for (File logFile : logFiles) {
      String name = logFile.getName();
      metaDataWriters.add(LogFileFactory.getMetaDataWriter(logFile,
          Integer.parseInt(name.substring(name.lastIndexOf('-') + 1))));
    }
    try {
      if (queue.checkpoint(true)) {
        checkpointLogOrderID = queue.getLogWriteOrderID();
        for (LogFile.MetaDataWriter metaDataWriter : metaDataWriters) {
          metaDataWriter.markCheckpoint(checkpointLogOrderID);
        }
      }
    } catch (Exception e) {
      LOG.warn("Error while generating checkpoint using fast generation logic", e);
    } finally {
      for (LogFile.MetaDataWriter metaDataWriter : metaDataWriters) {
        metaDataWriter.close();
      }
    }
  }

  private final class ComparableFlumeEventPointer
      implements Comparable<ComparableFlumeEventPointer> {

    private final FlumeEventPointer pointer;
    private final long orderID;

    public ComparableFlumeEventPointer(FlumeEventPointer pointer, long orderID) {
      Preconditions.checkNotNull(pointer, "FlumeEventPointer cannot be"
          + "null while creating a ComparableFlumeEventPointer");
      this.pointer = pointer;
      this.orderID = orderID;
    }

    @Override
    public int compareTo(ComparableFlumeEventPointer o) {
      if (orderID < o.orderID) {
        return -1;
      } else { //Unfortunately same log order id does not mean same event
        //for older logs.
        return 1;
      }
    }

    @Override
    public int hashCode() {
      return pointer.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null) {
        return false;
      }
      if (o.getClass() != this.getClass()) {
        return false;
      }
      return pointer.equals(((ComparableFlumeEventPointer) o).pointer);
    }
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    Option opt = new Option("c", true, "checkpoint directory");
    opt.setRequired(true);
    options.addOption(opt);
    opt = new Option("l", true, "comma-separated list of log directories");
    opt.setRequired(true);
    options.addOption(opt);
    options.addOption(opt);
    opt = new Option("t", true, "capacity of the channel");
    opt.setRequired(true);
    options.addOption(opt);
    CommandLineParser parser = new GnuParser();
    CommandLine cli = parser.parse(options, args);
    File checkpointDir = new File(cli.getOptionValue("c"));
    String[] logDirs = cli.getOptionValue("l").split(",");
    List<File> logFiles = Lists.newArrayList();
    for (String logDir : logDirs) {
      logFiles.addAll(LogUtils.getLogs(new File(logDir)));
    }
    int capacity = Integer.parseInt(cli.getOptionValue("t"));
    File checkpointFile = new File(checkpointDir, "checkpoint");
    if (checkpointFile.exists()) {
      LOG.error("Cannot execute fast replay",
                new IllegalStateException("Checkpoint exists" + checkpointFile));
    } else {
      EventQueueBackingStore backingStore =
          EventQueueBackingStoreFactory.get(checkpointFile,
              capacity, "channel");
      FlumeEventQueue queue = new FlumeEventQueue(backingStore,
          new File(checkpointDir, "inflighttakes"),
          new File(checkpointDir, "inflightputs"),
          new File(checkpointDir, Log.QUEUE_SET));
      CheckpointRebuilder rebuilder = new CheckpointRebuilder(logFiles, queue, true);
      if (rebuilder.rebuild()) {
        rebuilder.writeCheckpoint();
      } else {
        LOG.error("Could not rebuild the checkpoint due to errors.");
      }
    }
  }
}
