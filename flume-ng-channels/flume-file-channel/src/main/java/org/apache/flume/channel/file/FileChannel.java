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
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.Disposable;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.channel.file.Log.Builder;
import org.apache.flume.channel.file.encryption.EncryptionConfiguration;
import org.apache.flume.channel.file.encryption.KeyProvider;
import org.apache.flume.channel.file.encryption.KeyProviderFactory;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * A durable {@link Channel} implementation that uses the local file system for
 * its storage.
 * </p>
 * <p>
 * FileChannel works by writing all transactions to a set of directories
 * specified in the configuration. Additionally, when a commit occurs
 * the transaction is synced to disk.
 * </p>
 * <p>
 * FileChannel is marked
 * {@link org.apache.flume.annotations.InterfaceAudience.Private} because it
 * should only be instantiated via a configuration. For example, users should
 * certainly use FileChannel but not by instantiating FileChannel objects.
 * Meaning the label Private applies to user-developers not user-operators.
 * In cases where a Channel is required by instantiated by user-developers
 * {@link org.apache.flume.channel.MemoryChannel} should be used.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
@Disposable
public class FileChannel extends BasicChannelSemantics {

  private static final Logger LOG = LoggerFactory.getLogger(FileChannel.class);

  private Integer capacity = 0;
  private int keepAlive;
  protected Integer transactionCapacity = 0;
  private Long checkpointInterval = 0L;
  private long maxFileSize;
  private long minimumRequiredSpace;
  private File checkpointDir;
  private File backupCheckpointDir;
  private File[] dataDirs;
  private Log log;
  private volatile boolean open;
  private volatile Throwable startupError;
  private Semaphore queueRemaining;
  private final ThreadLocal<FileBackedTransaction> transactions =
      new ThreadLocal<FileBackedTransaction>();
  private String channelNameDescriptor = "[channel=unknown]";
  private ChannelCounter channelCounter;
  private boolean useLogReplayV1;
  private boolean useFastReplay = false;
  private KeyProvider encryptionKeyProvider;
  private String encryptionActiveKey;
  private String encryptionCipherProvider;
  private boolean useDualCheckpoints;
  private boolean compressBackupCheckpoint;
  private boolean fsyncPerTransaction;
  private int fsyncInterval;
  private boolean checkpointOnClose = true;

  @Override
  public synchronized void setName(String name) {
    channelNameDescriptor = "[channel=" + name + "]";
    super.setName(name);
  }

  // 主要的配置方法
  @Override
  public void configure(Context context) {

    // 是否需要备份Checkpoint，默认false
    useDualCheckpoints = context.getBoolean(
        FileChannelConfiguration.USE_DUAL_CHECKPOINTS,
        FileChannelConfiguration.DEFAULT_USE_DUAL_CHECKPOINTS);

    // 是否压缩备份节点，默认false
    compressBackupCheckpoint = context.getBoolean(
        FileChannelConfiguration.COMPRESS_BACKUP_CHECKPOINT,
        FileChannelConfiguration.DEFAULT_COMPRESS_BACKUP_CHECKPOINT);

    // 用户家目录
    String homePath = System.getProperty("user.home").replace('\\', '/');

    // Checkpoint目录，默认为${user.home}/.flume/file-channel/checkpoint
    String strCheckpointDir =
        context.getString(FileChannelConfiguration.CHECKPOINT_DIR,
            homePath + "/.flume/file-channel/checkpoint").trim();

    // Checkpoint备份目录，默认为空
    String strBackupCheckpointDir =
        context.getString(FileChannelConfiguration.BACKUP_CHECKPOINT_DIR, "").trim();

    // 存储日志文件的数据目录，默认是${user.home}/.flume/file-channel/data
    String[] strDataDirs = Iterables.toArray(
        Splitter.on(",").trimResults().omitEmptyStrings().split(
            context.getString(FileChannelConfiguration.DATA_DIRS,
                homePath + "/.flume/file-channel/data")), String.class);

    // Checkpoint目录的File对象
    checkpointDir = new File(strCheckpointDir);

    // 如果需要两份Checkpoint，就创建备份Checkpoint目录的File对象
    if (useDualCheckpoints) {
      Preconditions.checkState(!strBackupCheckpointDir.isEmpty(),
          "Dual checkpointing is enabled, but the backup directory is not set. " +
              "Please set " + FileChannelConfiguration.BACKUP_CHECKPOINT_DIR + " " +
              "to enable dual checkpointing");
      backupCheckpointDir = new File(strBackupCheckpointDir);
      /*
       * If the backup directory is the same as the checkpoint directory,
       * then throw an exception and force the config system to ignore this
       * channel.
       *
       * Checkpoint目录和备份目录不可相同
       */
      Preconditions.checkState(!backupCheckpointDir.equals(checkpointDir),
          "Could not configure " + getName() + ". The checkpoint backup " +
              "directory and the checkpoint directory are " +
              "configured to be the same.");
    }

    // 创建数据目录File对象数组
    dataDirs = new File[strDataDirs.length];
    for (int i = 0; i < strDataDirs.length; i++) {
      dataDirs[i] = new File(strDataDirs[i]);
    }

    // FileChannel最大容量，默认1000000
    capacity = context.getInteger(FileChannelConfiguration.CAPACITY,
        FileChannelConfiguration.DEFAULT_CAPACITY);
    if (capacity <= 0) {
      capacity = FileChannelConfiguration.DEFAULT_CAPACITY;
      LOG.warn("Invalid capacity specified, initializing channel to "
          + "default capacity of {}", capacity);
    }

    // 等待Put操作的keepAlive时间，默认3秒
    keepAlive =
        context.getInteger(FileChannelConfiguration.KEEP_ALIVE,
            FileChannelConfiguration.DEFAULT_KEEP_ALIVE);

    // 事务容量，默认10000
    transactionCapacity =
        context.getInteger(FileChannelConfiguration.TRANSACTION_CAPACITY,
            FileChannelConfiguration.DEFAULT_TRANSACTION_CAPACITY);

    if (transactionCapacity <= 0) {
      transactionCapacity =
          FileChannelConfiguration.DEFAULT_TRANSACTION_CAPACITY;
      LOG.warn("Invalid transaction capacity specified, " +
          "initializing channel to default " +
          "capacity of {}", transactionCapacity);
    }

    // capacity的值一定要大于transactionCapacity
    Preconditions.checkState(transactionCapacity <= capacity,
        "File Channel transaction capacity cannot be greater than the " +
            "capacity of the channel.");

    // Checkpoint操作的间隔时间，默认30秒
    checkpointInterval =
        context.getLong(FileChannelConfiguration.CHECKPOINT_INTERVAL,
            FileChannelConfiguration.DEFAULT_CHECKPOINT_INTERVAL);
    if (checkpointInterval <= 0) {
      LOG.warn("Checkpoint interval is invalid: " + checkpointInterval
          + ", using default: "
          + FileChannelConfiguration.DEFAULT_CHECKPOINT_INTERVAL);

      checkpointInterval =
          FileChannelConfiguration.DEFAULT_CHECKPOINT_INTERVAL;
    }

    // cannot be over FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE
    // 最大的单个日志文件大小，默认Integer.MAX_VALUE - (500L * 1024L * 1024L); // ~1.52G
    maxFileSize = Math.min(
        context.getLong(FileChannelConfiguration.MAX_FILE_SIZE,
            FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE),
        FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE);

    // 最小的空闲空间，为了避免数据损坏，当空闲空间低于该值时会停止接收Take/Put请求；默认500MB
    minimumRequiredSpace = Math.max(
        context.getLong(FileChannelConfiguration.MINIMUM_REQUIRED_SPACE,
            FileChannelConfiguration.DEFAULT_MINIMUM_REQUIRED_SPACE),
        FileChannelConfiguration.FLOOR_MINIMUM_REQUIRED_SPACE);

    // 是否使用老的WAL回放逻辑，默认false
    useLogReplayV1 = context.getBoolean(
        FileChannelConfiguration.USE_LOG_REPLAY_V1,
        FileChannelConfiguration.DEFAULT_USE_LOG_REPLAY_V1);

    /**
     * 是否使用快速WAL回放机制，此机制不使用队列，默认false
     * 快速重放只有在Checkpoint文件不存在时才可以使用
     */
    useFastReplay = context.getBoolean(
        FileChannelConfiguration.USE_FAST_REPLAY,
        FileChannelConfiguration.DEFAULT_USE_FAST_REPLAY);

    // 加密上下文模块限定字符
    Context encryptionContext = new Context(
        context.getSubProperties(EncryptionConfiguration.ENCRYPTION_PREFIX +
            "."));

    // 密钥供应商类型，支持JCEKSFILE
    String encryptionKeyProviderName = encryptionContext.getString(
        EncryptionConfiguration.KEY_PROVIDER);

    // 用于加密的密钥的名称
    encryptionActiveKey = encryptionContext.getString(
        EncryptionConfiguration.ACTIVE_KEY);

    // Cipher加密提供程序类型，支持AESCTRNOPADDING
    encryptionCipherProvider = encryptionContext.getString(
        EncryptionConfiguration.CIPHER_PROVIDER);
    if (encryptionKeyProviderName != null) {
      // 密钥不能为空
      Preconditions.checkState(!Strings.isNullOrEmpty(encryptionActiveKey),
          "Encryption configuration problem: " +
              EncryptionConfiguration.ACTIVE_KEY + " is missing");
      // Cipher密钥提供者类型不能为空
      Preconditions.checkState(!Strings.isNullOrEmpty(encryptionCipherProvider),
          "Encryption configuration problem: " +
              EncryptionConfiguration.CIPHER_PROVIDER + " is missing");

      // 密钥提供者上下文
      Context keyProviderContext = new Context(
          encryptionContext.getSubProperties(EncryptionConfiguration.KEY_PROVIDER + "."));
      // 创建密钥加密提供者
      encryptionKeyProvider = KeyProviderFactory.getInstance(
          encryptionKeyProviderName, keyProviderContext);
    } else {
      Preconditions.checkState(encryptionActiveKey == null,
          "Encryption configuration problem: " +
              EncryptionConfiguration.ACTIVE_KEY + " is present while key " +
              "provider name is not.");
      Preconditions.checkState(encryptionCipherProvider == null,
          "Encryption configuration problem: " +
              EncryptionConfiguration.CIPHER_PROVIDER + " is present while " +
              "key provider name is not.");
    }

    // 每次事务是否都fsync同步，默认true
    fsyncPerTransaction = context.getBoolean(FileChannelConfiguration
        .FSYNC_PER_TXN, FileChannelConfiguration.DEFAULT_FSYNC_PRE_TXN);

    // fsync同步间隔时间，默认5秒
    fsyncInterval = context.getInteger(FileChannelConfiguration
        .FSYNC_INTERVAL, FileChannelConfiguration.DEFAULT_FSYNC_INTERVAL);

    // 是否在关闭时做Checkpoint，默认true
    checkpointOnClose = context.getBoolean(FileChannelConfiguration
        .CHKPT_ONCLOSE, FileChannelConfiguration.DEFAULT_CHKPT_ONCLOSE);

    // 创建剩余队列信号量
    if (queueRemaining == null) {
      queueRemaining = new Semaphore(capacity, true);
    }
    if (log != null) {
      // 设置Checkpoint间隔
      log.setCheckpointInterval(checkpointInterval);
      // 设置最大日志文件大小
      log.setMaxFileSize(maxFileSize);
    }

    // 计数器
    if (channelCounter == null) {
      channelCounter = new ChannelCounter(getName());
    }
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting {}...", this);
    try {
      // 根据配置参数得到构建器
      Builder builder = new Log.Builder();
      builder.setCheckpointInterval(checkpointInterval);
      builder.setMaxFileSize(maxFileSize);
      builder.setMinimumRequiredSpace(minimumRequiredSpace);
      builder.setQueueSize(capacity);
      builder.setCheckpointDir(checkpointDir);
      builder.setLogDirs(dataDirs);
      builder.setChannelName(getName());
      builder.setUseLogReplayV1(useLogReplayV1);
      builder.setUseFastReplay(useFastReplay);
      builder.setEncryptionKeyProvider(encryptionKeyProvider);
      builder.setEncryptionKeyAlias(encryptionActiveKey);
      builder.setEncryptionCipherProvider(encryptionCipherProvider);
      builder.setUseDualCheckpoints(useDualCheckpoints);
      builder.setCompressBackupCheckpoint(compressBackupCheckpoint);
      builder.setBackupCheckpointDir(backupCheckpointDir);
      builder.setFsyncPerTransaction(fsyncPerTransaction);
      builder.setFsyncInterval(fsyncInterval);
      builder.setCheckpointOnClose(checkpointOnClose);

      // 创建Log对象
      log = builder.build();

      // 先进行重放，会尝试获取checkpointDir和dataDir文件锁
      log.replay();

      // 标记Channel已经打开
      open = true;

      // 获取还未处理的Event数量
      int depth = getDepth();
      Preconditions.checkState(queueRemaining.tryAcquire(depth),
          "Unable to acquire " + depth + " permits " + channelNameDescriptor);
      LOG.info("Queue Size after replay: " + depth + " "
          + channelNameDescriptor);
    } catch (Throwable t) {
      open = false;
      startupError = t;
      LOG.error("Failed to start the file channel " + channelNameDescriptor, t);
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
    if (open) {
      // 启动计数器
      channelCounter.start();
      channelCounter.setChannelSize(getDepth());
      channelCounter.setChannelCapacity(capacity);
    }
    super.start();
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping {}...", this);
    startupError = null;
    int size = getDepth();
    close();
    if (!open) {
      channelCounter.setChannelSize(size);
      channelCounter.stop();
    }
    super.stop();
  }

  @Override
  public String toString() {
    return "FileChannel " + getName() + " { dataDirs: " +
        Arrays.toString(dataDirs) + " }";
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    if (!open) {
      String msg = "Channel closed " + channelNameDescriptor;
      if (startupError != null) {
        msg += ". Due to " + startupError.getClass().getName() + ": " +
            startupError.getMessage();
        throw new IllegalStateException(msg, startupError);
      }
      throw new IllegalStateException(msg);
    }

    FileBackedTransaction trans = transactions.get();
    if (trans != null && !trans.isClosed()) {
      Preconditions.checkState(false,
          "Thread has transaction which is still open: " +
              trans.getStateAsString() + channelNameDescriptor);
    }
    trans = new FileBackedTransaction(log, TransactionIDOracle.next(),
        transactionCapacity, keepAlive, queueRemaining, getName(),
        fsyncPerTransaction, channelCounter);
    transactions.set(trans);
    return trans;
  }

  protected int getDepth() {
    Preconditions.checkState(open, "Channel closed" + channelNameDescriptor);
    Preconditions.checkNotNull(log, "log");
    FlumeEventQueue queue = log.getFlumeEventQueue();
    Preconditions.checkNotNull(queue, "queue");
    return queue.getSize();
  }

  void close() {
    if (open) {
      open = false;
      try {
        log.close();
      } catch (Exception e) {
        LOG.error("Error while trying to close the log.", e);
        Throwables.propagate(e);
      }
      log = null;
      queueRemaining = null;
    }
  }

  @VisibleForTesting
  boolean didFastReplay() {
    return log.didFastReplay();
  }


  @VisibleForTesting
  boolean didFullReplayDueToBadCheckpointException() {
    return log.didFullReplayDueToBadCheckpointException();
  }

  public boolean isOpen() {
    return open;
  }

  /**
   * Did this channel recover a backup of the checkpoint to restart?
   *
   * @return true if the channel recovered using a backup.
   */
  @VisibleForTesting
  boolean checkpointBackupRestored() {
    if (log != null) {
      return log.backupRestored();
    }
    return false;
  }

  @VisibleForTesting
  Log getLog() {
    return log;
  }

  /**
   * Transaction backed by a file. This transaction supports either puts
   * or takes but not both.
   */
  static class FileBackedTransaction extends BasicTransactionSemantics {
    private final LinkedBlockingDeque<FlumeEventPointer> takeList;
    private final LinkedBlockingDeque<FlumeEventPointer> putList;
    private final long transactionID;
    private final int keepAlive;
    private final Log log;
    private final FlumeEventQueue queue;
    private final Semaphore queueRemaining;
    private final String channelNameDescriptor;
    private final ChannelCounter channelCounter;
    private final boolean fsyncPerTransaction;

    public FileBackedTransaction(Log log, long transactionID,
                                 int transCapacity, int keepAlive, Semaphore queueRemaining,
                                 String name, boolean fsyncPerTransaction, ChannelCounter
                                     counter) {
      this.log = log;
      queue = log.getFlumeEventQueue();
      this.transactionID = transactionID;
      this.keepAlive = keepAlive;
      this.queueRemaining = queueRemaining;
      putList = new LinkedBlockingDeque<FlumeEventPointer>(transCapacity);
      takeList = new LinkedBlockingDeque<FlumeEventPointer>(transCapacity);
      this.fsyncPerTransaction = fsyncPerTransaction;
      channelNameDescriptor = "[channel=" + name + "]";
      this.channelCounter = counter;
    }

    private boolean isClosed() {
      return State.CLOSED.equals(getState());
    }

    private String getStateAsString() {
      return String.valueOf(getState());
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
      channelCounter.incrementEventPutAttemptCount();
      if (putList.remainingCapacity() == 0) { // 是否putList队列是否有剩余空间，大小为事务总量
        throw new ChannelException("Put queue for FileBackedTransaction " +
            "of capacity " + putList.size() + " full, consider " +
            "committing more frequently, increasing capacity or " +
            "increasing thread count. " + channelNameDescriptor);
      }
      // this does not need to be in the critical section as it does not
      // modify the structure of the log or queue.
      /**
       * 尝试获取一个信号量许可，会阻塞等待；
       * 信号量总许可数量即为capacity，如果获取不到，说明队列满了。
       */
      if (!queueRemaining.tryAcquire(keepAlive, TimeUnit.SECONDS)) {
        throw new ChannelFullException("The channel has reached it's capacity. "
            + "This might be the result of a sink on the channel having too "
            + "low of batch size, a downstream system running slower than "
            + "normal, or that the channel capacity is just too low. "
            + channelNameDescriptor);
      }
      boolean success = false;
      // 获取读共享锁
      log.lockShared();
      try {
        // transactionID是在TransactionIDOracle类中递增的
        FlumeEventPointer ptr = log.put(transactionID, event);
        Preconditions.checkState(putList.offer(ptr), "putList offer failed "
            + channelNameDescriptor);
        // 存入inflightPuts队列及Checkpoint的inflightputs文件
        queue.addWithoutCommit(ptr, transactionID);
        success = true;
      } catch (IOException e) {
        throw new ChannelException("Put failed due to IO error "
            + channelNameDescriptor, e);
      } finally {
        log.unlockShared();
        if (!success) {
          // release slot obtained in the case
          // the put fails for any reason
          // 如果未成功，释放信号量
          queueRemaining.release();
        }
      }
    }

    @Override
    protected Event doTake() throws InterruptedException {
      channelCounter.incrementEventTakeAttemptCount();
      if (takeList.remainingCapacity() == 0) {
        throw new ChannelException("Take list for FileBackedTransaction, capacity " +
            takeList.size() + " full, consider committing more frequently, " +
            "increasing capacity, or increasing thread count. "
            + channelNameDescriptor);
      }
      log.lockShared();
      /*
       * 1. Take an event which is in the queue.
       * 2. If getting that event does not throw NoopRecordException,
       *    then return it.
       * 3. Else try to retrieve the next event from the queue
       * 4. Repeat 2 and 3 until queue is empty or an event is returned.
       */

      try {
        while (true) {
          // 从checkpoint文件内容所构造的队列中取出队头已经提交的事件，该操作会将Take存入到inflightTakes
          FlumeEventPointer ptr = queue.removeHead(transactionID);
          if (ptr == null) { // 未取到，返回null
            return null;
          } else { // 取到了
            try {
              // first add to takeList so that if write to disk
              // fails rollback actually does it's work
              // 存入takeList队列
              Preconditions.checkState(takeList.offer(ptr),
                  "takeList offer failed "
                      + channelNameDescriptor);
              // 将TAKE操作写入到Log日志文件
              log.take(transactionID, ptr); // write take to disk
              // 从Log日志文件中取出对应的事件，并返回
              Event event = log.get(ptr);
              return event;
            } catch (IOException e) {
              throw new ChannelException("Take failed due to IO error "
                  + channelNameDescriptor, e);
            } catch (NoopRecordException e) {
              LOG.warn("Corrupt record replaced by File Channel Integrity " +
                  "tool found. Will retrieve next event", e);
              takeList.remove(ptr);
            } catch (CorruptEventException ex) {
              if (fsyncPerTransaction) {
                throw new ChannelException(ex);
              }
              LOG.warn("Corrupt record found. Event will be " +
                  "skipped, and next event will be read.", ex);
              takeList.remove(ptr);
            }
          }
        }
      } finally {
        log.unlockShared();
      }
    }

    /**
     * 提交操作是批量提交，前面不管有多少PUT/TAKE还未提交，只要COMMIT，就会一次性全部提交。
     * PUT/TAKE不可能同时存在，也即是，一次事务中，不能PUT了未提交，然后直接TAKE。
     * 相对于Source，COMMIT的全是PUT，相对于Sink，COMMIT的全是TAKE，
     * 而Source和Sink是各自有自己的事务的，互不交叉。
     * @throws InterruptedException
     */
    @Override
    protected void doCommit() throws InterruptedException {
      // 获取putList和takeList的大小
      int puts = putList.size();
      int takes = takeList.size();
      if (puts > 0) { // 有等待处理的PUT
        // 如果有PUT也有TAKE，则报错
        Preconditions.checkState(takes == 0, "nonzero puts and takes "
            + channelNameDescriptor);
        // 获取读锁
        log.lockShared();
        try {
          // COMMIT当前事务的PUT
          log.commitPut(transactionID);
          // PUT计数更新
          channelCounter.addToEventPutSuccessCount(puts);
          synchronized (queue) {
            // 将putList中的PUT全部放入FlumeEventQueue尾部，表示全部提交
            while (!putList.isEmpty()) {
              if (!queue.addTail(putList.removeFirst())) {
                StringBuilder msg = new StringBuilder();
                msg.append("Queue add failed, this shouldn't be able to ");
                msg.append("happen. A portion of the transaction has been ");
                msg.append("added to the queue but the remaining portion ");
                msg.append("cannot be added. Those messages will be consumed ");
                msg.append("despite this transaction failing. Please report.");
                msg.append(channelNameDescriptor);
                LOG.error(msg.toString());
                Preconditions.checkState(false, msg.toString());
              }
            }
            // 尝试清空Checkpoint文件夹中inflightputs和inflighttakes文件的内容
            queue.completeTransaction(transactionID);
          }
        } catch (IOException e) {
          throw new ChannelException("Commit failed due to IO error "
              + channelNameDescriptor, e);
        } finally {
          // 释放锁
          log.unlockShared();
        }

      } else if (takes > 0) {
        // 获取读锁
        log.lockShared();
        try {
          // 写入Log日志文件
          log.commitTake(transactionID);
          // 尝试清空Checkpoint文件夹中inflightputs和inflighttakes文件的内容
          queue.completeTransaction(transactionID);
          // 更新Take计数器
          channelCounter.addToEventTakeSuccessCount(takes);
        } catch (IOException e) {
          throw new ChannelException("Commit failed due to IO error "
              + channelNameDescriptor, e);
        } finally {
          // 释放锁
          log.unlockShared();
        }
        // 因为有Take操作被提交了，说明有数据被取走了，此时应该释放对应的容量
        queueRemaining.release(takes);
      }
      // 清理putList和takeList
      putList.clear();
      takeList.clear();
      channelCounter.setChannelSize(queue.getSize());
    }

    @Override
    protected void doRollback() throws InterruptedException {
      // 获取putList和takeList的大小
      int puts = putList.size();
      int takes = takeList.size();
      // 获取读锁
      log.lockShared();
      try {
        if (takes > 0) {
          // 如果有TAKE也有PUT，则报错
          Preconditions.checkState(puts == 0, "nonzero puts and takes "
              + channelNameDescriptor);
          synchronized (queue) {
            // 将需要回滚的Take操作全部添加到FlumeEventQueue的队首
            while (!takeList.isEmpty()) {
              Preconditions.checkState(queue.addHead(takeList.removeLast()),
                  "Queue add failed, this shouldn't be able to happen "
                      + channelNameDescriptor);
            }
          }
        }

        // 清空两个队列
        putList.clear();
        takeList.clear();
        // 尝试清空Checkpoint文件夹中inflightputs和inflighttakes文件的内容
        queue.completeTransaction(transactionID);
        channelCounter.setChannelSize(queue.getSize());
        // 封装成ByteBuffer，写入到缓存文件中
        log.rollback(transactionID);
      } catch (IOException e) {
        throw new ChannelException("Commit failed due to IO error "
            + channelNameDescriptor, e);
      } finally {
        log.unlockShared();
        // since rollback is being called, puts will never make it on
        // to the queue and we need to be sure to release the resources
        // 因为回滚了，所以Put都丢弃了，因此需要释放对应的容量
        queueRemaining.release(puts);
      }
    }
  }
}
