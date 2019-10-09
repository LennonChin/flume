/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source.taildir;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.apache.flume.source.PollableSourceRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;

public class TaildirSource extends AbstractSource implements
        PollableSource, Configurable {

  private static final Logger logger = LoggerFactory.getLogger(TaildirSource.class);

  private Map<String, String> filePaths;
  private Table<String, String, String> headerTable;
  private int batchSize;
  private String positionFilePath;
  private boolean skipToEnd;
  private boolean byteOffsetHeader;

  private SourceCounter sourceCounter;
  private ReliableTaildirEventReader reader;
  private ScheduledExecutorService idleFileChecker;
  private ScheduledExecutorService positionWriter;
  private int retryInterval = 1000;
  private int maxRetryInterval = 5000;
  private int idleTimeout;
  private int checkIdleInterval = 5000;
  private int writePosInitDelay = 5000;
  private int writePosInterval;
  private boolean cachePatternMatching;

  private List<Long> existingInodes = new CopyOnWriteArrayList<Long>();
  private List<Long> idleInodes = new CopyOnWriteArrayList<Long>();
  private Long backoffSleepIncrement;
  private Long maxBackOffSleepInterval;
  private boolean fileHeader;
  private String fileHeaderKey;

  // 主要的启动方法
  @Override
  public synchronized void start() {
    logger.info("{} TaildirSource source starting with directory: {}", getName(), filePaths);
    try {
      reader = new ReliableTaildirEventReader.Builder()
              .filePaths(filePaths)
              .headerTable(headerTable)
              .positionFilePath(positionFilePath)
              .skipToEnd(skipToEnd)
              .addByteOffset(byteOffsetHeader)
              .cachePatternMatching(cachePatternMatching)
              .annotateFileName(fileHeader)
              .fileNameHeader(fileHeaderKey)
              .build();
    } catch (IOException e) {
      throw new FlumeException("Error instantiating ReliableTaildirEventReader", e);
    }

    // 监控日志文件是否空闲的线程池，单线程池
    idleFileChecker = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("idleFileChecker").build());

    // 延迟idleTimeout（120秒）启动，每隔checkIdleInterval（5秒）间隔调用1次
    idleFileChecker.scheduleWithFixedDelay(new idleFileCheckerRunnable(),
            idleTimeout, checkIdleInterval, TimeUnit.MILLISECONDS);

    // 用于向positionFile偏移量文件记录偏移量的线程池，单线程池
    positionWriter = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("positionWriter").build());
    positionWriter.scheduleWithFixedDelay(new PositionWriterRunnable(),
            writePosInitDelay, writePosInterval, TimeUnit.MILLISECONDS);

    // 启动，更新状态
    super.start();
    logger.debug("TaildirSource started");
    sourceCounter.start();
  }

  @Override
  public synchronized void stop() {
    try {
      super.stop();
      ExecutorService[] services = {idleFileChecker, positionWriter};
      for (ExecutorService service : services) {
        service.shutdown();
        if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          service.shutdownNow();
        }
      }
      // write the last position
      writePosition();
      reader.close();
    } catch (InterruptedException e) {
      logger.info("Interrupted while awaiting termination", e);
    } catch (IOException e) {
      logger.info("Failed: " + e.getMessage(), e);
    }
    sourceCounter.stop();
    logger.info("Taildir source {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  @Override
  public String toString() {
    return String.format("Taildir source: { positionFile: %s, skipToEnd: %s, "
                    + "byteOffsetHeader: %s, idleTimeout: %s, writePosInterval: %s }",
            positionFilePath, skipToEnd, byteOffsetHeader, idleTimeout, writePosInterval);
  }

  // 配置方法
  @Override
  public synchronized void configure(Context context) {
    // 以空格分割的文件组列表，每个文件组否指示一组要挂起的文件，FILE_GROUPS即是字符串"filegroups"
    String fileGroups = context.getString(FILE_GROUPS);

    // 检查fileGroups属性是否为空
    Preconditions.checkState(fileGroups != null, "Missing param: " + FILE_GROUPS);

    /**
     * 从context.getSubProperties(FILE_GROUPS_PREFIX)中挑选fileGroups中的键
     * 返回一个group对应FilePath的Map<String, String>
     */
    filePaths = selectByKeys(context.getSubProperties(FILE_GROUPS_PREFIX),
            fileGroups.split("\\s+"));

    // 判断文件路径是否存在
    Preconditions.checkState(!filePaths.isEmpty(),
            "Mapping for tailing files is empty or invalid: '" + FILE_GROUPS_PREFIX + "'");

    // 获取用户家目录
    String homePath = System.getProperty("user.home").replace('\\', '/');

    // 获取positionFile路径，默认路径为${user.home}/.flume/taildir_position.json
    positionFilePath = context.getString(POSITION_FILE, homePath + DEFAULT_POSITION_FILE);

    // 得到positionFile路径的Path对象
    Path positionFile = Paths.get(positionFilePath);
    try {
      // 递归创建positionFile的父目录
      Files.createDirectories(positionFile.getParent());
    } catch (IOException e) {
      throw new FlumeException("Error creating positionFile parent directories", e);
    }

    // 用于发送Event的header信息添加值
    headerTable = getTable(context, HEADERS_PREFIX);
    // 批量大小，默认100
    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    // 从头还是从尾部读取，默认false
    skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);
    // 是否加偏移量，剔除每行日志的标题，默认false
    byteOffsetHeader = context.getBoolean(BYTE_OFFSET_HEADER, DEFAULT_BYTE_OFFSET_HEADER);
    // 空闲超时时间，当Source日志文件超过这个时间没被修改，就会被关闭，默认120000，即120秒
    idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
    /**
     * 在读取每个监控文件时，TailDirSource都在positionFile文件中记录监控文件的已经读取的偏移量,
     * 该值表示更新positionFile的间隔时间，默认3000，即3秒
     */
    writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);

    // 是否开启Matcher Cache，默认开启
    cachePatternMatching = context.getBoolean(CACHE_PATTERN_MATCHING,
            DEFAULT_CACHE_PATTERN_MATCHING);

    // 无更新数据时的，退避增量时间，默认1000，即1秒
    backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
            PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
    // 无更新数据时的，最大的退避时间，默认5000，即5秒
    maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
            PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
    // 是否添加file header来记录日志文件的绝对路径
    fileHeader = context.getBoolean(FILENAME_HEADER,
            DEFAULT_FILE_HEADER);
    // 添加file header时的key
    fileHeaderKey = context.getString(FILENAME_HEADER_KEY,
            DEFAULT_FILENAME_HEADER_KEY);

    // 计数器
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  private Map<String, String> selectByKeys(Map<String, String> map, String[] keys) {
    Map<String, String> result = Maps.newHashMap();
    for (String key : keys) {
      if (map.containsKey(key)) {
        result.put(key, map.get(key));
      }
    }
    return result;
  }

  private Table<String, String, String> getTable(Context context, String prefix) {
    Table<String, String, String> table = HashBasedTable.create();
    for (Entry<String, String> e : context.getSubProperties(prefix).entrySet()) {
      String[] parts = e.getKey().split("\\.", 2);
      table.put(parts[0], parts[1], e.getValue());
    }
    return table;
  }

  @VisibleForTesting
  protected SourceCounter getSourceCounter() {
    return sourceCounter;
  }

  /**
   * 该方法是处理日志的主要方法，
   * 会在 {@link PollableSourceRunner.PollingRunner#run()} 方法中被调用，
   * 会被不断循环调用
   */
  @Override
  public Status process() {
    Status status = Status.READY;
    try {
      // 清理existingInodes列表
      existingInodes.clear();
      // 将ReliableTaildirEventReader中读取到的文件的inode添加到existingInodes
      existingInodes.addAll(reader.updateTailFiles());
      // 遍历文件的inode
      for (long inode : existingInodes) {
        // 获取对应的TailFile
        TailFile tf = reader.getTailFiles().get(inode);
        // 判断文件是否需要被Tail操作
        if (tf.needTail()) {
          // 进行Tail操作
          tailFileProcess(tf, true);
        }
      }
      // 关闭空闲的TailFile
      closeTailFiles();
      try {
        TimeUnit.MILLISECONDS.sleep(retryInterval);
      } catch (InterruptedException e) {
        logger.info("Interrupted while sleeping");
      }
    } catch (Throwable t) {
      logger.error("Unable to tail files", t);
      status = Status.BACKOFF;
    }
    return status;
  }

  @Override
  public long getBackOffSleepIncrement() {
    return backoffSleepIncrement;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return maxBackOffSleepInterval;
  }

  // 处理需要Tail操作的文件
  private void tailFileProcess(TailFile tf, boolean backoffWithoutNL)
          throws IOException, InterruptedException {

    // 不断循环
    while (true) {
      // 设置当前处理的TailFile
      reader.setCurrentFile(tf);

      // 读取batchSize个事件，返回事件列表
      List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);

      // 如果事件列表为空，结束循环
      if (events.isEmpty()) {
        break;
      }

      // 计数器更新，记录接收日志数量
      sourceCounter.addToEventReceivedCount(events.size());
      sourceCounter.incrementAppendBatchReceivedCount();
      try {

        // 获取Channel的channelProcessor，将事件传递给Channel
        getChannelProcessor().processEventBatch(events);
          /**
           * 提交操作，主要是更新当前处理的日志文件对应TailFile对象的pos和最后一次更新时间
           * 在后期会由定时任务定期将Position信息写入到Position File
           */
        reader.commit();
      } catch (ChannelException ex) {
        logger.warn("The channel is full or unexpected failure. " +
                "The source will try again after " + retryInterval + " ms");
        // 退避一段时间，内部是Thread.sleep()的实现，默认1秒
        TimeUnit.MILLISECONDS.sleep(retryInterval);
        // 下一次退避时间是上一次的2倍
        retryInterval = retryInterval << 1;
        // 最大退避时间由maxRetryInterval决定，5秒
        retryInterval = Math.min(retryInterval, maxRetryInterval);
        continue;
      }
      retryInterval = 1000;

      // 记录Channel接收日志数量
      sourceCounter.addToEventAcceptedCount(events.size());
      sourceCounter.incrementAppendBatchAcceptedCount();

      // 事件数量
      if (events.size() < batchSize) {
        break;
      }
    }
  }

  private void closeTailFiles() throws IOException, InterruptedException {
      // 遍历空闲File的inode
    for (long inode : idleInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      // 如果对应的raf不为空，则先进行一次Tail操作，然后关闭
      if (tf.getRaf() != null) { // when file has not closed yet
        tailFileProcess(tf, false);
        tf.close();
        logger.info("Closed file: " + tf.getPath() + ", inode: " + inode + ", pos: " + tf.getPos());
      }
    }
    // 清空空闲文件inode列表
    idleInodes.clear();
  }

  /**
   * Runnable class that checks whether there are files which should be closed.
   *
   * 文件空闲检测
   */
  private class idleFileCheckerRunnable implements Runnable {
    @Override
    public void run() {
      try {
        long now = System.currentTimeMillis();
        // 遍历所有TailFile
        for (TailFile tf : reader.getTailFiles().values()) {
          if (tf.getLastUpdated() + idleTimeout < now && tf.getRaf() != null) {
            idleInodes.add(tf.getInode());
          }
        }
      } catch (Throwable t) {
        logger.error("Uncaught exception in IdleFileChecker thread", t);
      }
    }
  }

  /**
   * Runnable class that writes a position file which has the last read position
   * of each file.
   */
  private class PositionWriterRunnable implements Runnable {
    @Override
    public void run() {
      // 写Position File
      writePosition();
    }
  }

  // 写偏移量的操作
  private void writePosition() {
    // 得到偏移量文件
    File file = new File(positionFilePath);
    FileWriter writer = null;
    try {
      writer = new FileWriter(file);
      if (!existingInodes.isEmpty()) {
        String json = toPosInfoJson();
        writer.write(json);
      }
    } catch (Throwable t) {
      logger.error("Failed writing positionFile", t);
    } finally {
      try {
        if (writer != null) writer.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
      }
    }
  }

  private String toPosInfoJson() {
    @SuppressWarnings("rawtypes")
    List<Map> posInfos = Lists.newArrayList();
    for (Long inode : existingInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(), "file", tf.getPath()));
    }
    return new Gson().toJson(posInfos);
  }
}
