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

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

class EventQueueBackingStoreFactory {
  private static final Logger LOG = LoggerFactory.getLogger(EventQueueBackingStoreFactory.class);

  private EventQueueBackingStoreFactory() {}

  static EventQueueBackingStore get(File checkpointFile, int capacity,
                                    String name) throws Exception {
    return get(checkpointFile, capacity, name, true);
  }

  static EventQueueBackingStore get(File checkpointFile, int capacity,
                                    String name, boolean upgrade) throws Exception {
    return get(checkpointFile, null, capacity, name, upgrade, false, false);
  }

  static EventQueueBackingStore get(File checkpointFile, File backupCheckpointDir,
                                    int capacity, String name, boolean upgrade,
                                    boolean shouldBackup, boolean compressBackup) throws Exception {
    // checkpoint.meta文件,${user.home}/.flume/file-channel/checkpoint/checkpoint.meta文件
    File metaDataFile = Serialization.getMetaDataFile(checkpointFile);
    RandomAccessFile checkpointFileHandle = null;
    try {
      // 检查checkpoint文件是否存在
      boolean checkpointExists = checkpointFile.exists();
      // 检查checkpoint.meta文件是否存在
      boolean metaDataExists = metaDataFile.exists();
      // 如果checkpoint.meta文件存在，checkpoint文件必须存在且长度大于0
      if (metaDataExists) { // checkpoint.meta文件存在
        // if we have a metadata file but no checkpoint file, we have a problem
        // delete everything in the checkpoint directory and force
        // a full replay.
        // 如果checkpoint文件不存在，或长度为0，则抛出异常
        if (!checkpointExists || checkpointFile.length() == 0) {
          LOG.warn("MetaData file for checkpoint "
              + " exists but checkpoint does not. Checkpoint = " + checkpointFile
              + ", metaDataFile = " + metaDataFile);
          throw new BadCheckpointException(
              "The last checkpoint was not completed correctly, " +
                  "since Checkpoint file does not exist while metadata " +
                  "file does.");
        }
      }
      // brand new, use v3
      if (!checkpointExists) { // checkpoint文件不存在
        if (!checkpointFile.createNewFile()) { // 创建checkpoint文件
          throw new IOException("Cannot create " + checkpointFile);
        }
        // 返回v3版本的EventQueueBackingStoreFileV3
        return new EventQueueBackingStoreFileV3(checkpointFile,
            capacity, name, backupCheckpointDir, shouldBackup, compressBackup);
      }
      // v3 due to meta file, version will be checked by backing store
      // v3版本的Checkpoint才有checkpoint.meta
      // checkpoint文件存在
      if (metaDataExists) { // checkpoint.meta文件存在
        // 返回V3版本的EventQueueBackingStoreFileV3
        return new EventQueueBackingStoreFileV3(checkpointFile, capacity,
            name, backupCheckpointDir, shouldBackup, compressBackup);
      }

      // checkpoint文件存在，但不存在checkpoint.meta文件
      // 创建checkpoint文件的RAF
      checkpointFileHandle = new RandomAccessFile(checkpointFile, "r");
      // 读Long长度的Version
      int version = (int) checkpointFileHandle.readLong();
      if (Serialization.VERSION_2 == version) { // 2版本
        if (upgrade) { // 升级，返回v3版本的EventQueueBackingStoreFileV3
          return upgrade(checkpointFile, capacity, name, backupCheckpointDir,
              shouldBackup, compressBackup);
        }
        // 不升级，返回v2版本的EventQueueBackingStoreFileV2
        return new EventQueueBackingStoreFileV2(checkpointFile, capacity, name);
      }
      LOG.error("Found version " + Integer.toHexString(version) + " in " +
          checkpointFile);
      throw new BadCheckpointException("Checkpoint file exists with " +
          Serialization.VERSION_3 + " but no metadata file found.");
    } finally {
      if (checkpointFileHandle != null) {
        try {
          checkpointFileHandle.close();
        } catch (IOException e) {
          LOG.warn("Unable to close " + checkpointFile, e);
        }
      }
    }
  }

  private static EventQueueBackingStore upgrade(File checkpointFile, int capacity, String name,
                                                File backupCheckpointDir, boolean shouldBackup,
                                                boolean compressBackup) throws Exception {
    LOG.info("Attempting upgrade of " + checkpointFile + " for " + name);
    // v2，EventQueueBackingStoreFileV2
    EventQueueBackingStoreFileV2 backingStoreV2 =
        new EventQueueBackingStoreFileV2(checkpointFile, capacity, name);
    String backupName = checkpointFile.getName() + "-backup-"
        + System.currentTimeMillis();

    // 把v2版本的checkpoint文件拷贝一份，命名为checkpoint-backup-当前时间戳
    Files.copy(checkpointFile,
        new File(checkpointFile.getParentFile(), backupName));

    // 构建checkpoint.meta文件的File对象
    File metaDataFile = Serialization.getMetaDataFile(checkpointFile);

    // 升级为V3操作
    EventQueueBackingStoreFileV3.upgrade(backingStoreV2, checkpointFile,
        metaDataFile);

    // 返回v3版本的EventQueueBackingStoreFileV3
    return new EventQueueBackingStoreFileV3(checkpointFile, capacity, name,
        backupCheckpointDir, shouldBackup, compressBackup);
  }

}
