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
import org.apache.flume.channel.file.encryption.KeyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.Key;

@SuppressWarnings("deprecation")
class LogFileFactory {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LogFileFactory.class);

  private LogFileFactory() {}

  static LogFile.MetaDataWriter getMetaDataWriter(File file, int logFileID)
      throws IOException {
    RandomAccessFile logFile = null;
    try {
      File metaDataFile = Serialization.getMetaDataFile(file);
      if (metaDataFile.exists()) {
        return new LogFileV3.MetaDataWriter(file, logFileID);
      }
      logFile = new RandomAccessFile(file, "r");
      int version = logFile.readInt();
      if (Serialization.VERSION_2 == version) {
        return new LogFileV2.MetaDataWriter(file, logFileID);
      }
      throw new IOException("File " + file + " has bad version " +
          Integer.toHexString(version));
    } finally {
      if (logFile != null) {
        try {
          logFile.close();
        } catch (IOException e) {
          LOGGER.warn("Unable to close " + file, e);
        }
      }
    }
  }

  static LogFile.Writer getWriter(File file, int logFileID,
                                  long maxFileSize, @Nullable Key encryptionKey,
                                  @Nullable String encryptionKeyAlias,
                                  @Nullable String encryptionCipherProvider,
                                  long usableSpaceRefreshInterval, boolean fsyncPerTransaction,
                                  int fsyncInterval) throws IOException {
    Preconditions.checkState(!file.exists(), "File already exists " +
        file.getAbsolutePath());
    Preconditions.checkState(file.createNewFile(), "File could not be created "
        + file.getAbsolutePath());
    return new LogFileV3.Writer(file, logFileID, maxFileSize, encryptionKey,
        encryptionKeyAlias, encryptionCipherProvider,
        usableSpaceRefreshInterval, fsyncPerTransaction, fsyncInterval);
  }

  static LogFile.RandomReader getRandomReader(File file,
                                              @Nullable KeyProvider encryptionKeyProvider,
                                              boolean fsyncPerTransaction)
      throws IOException {
    RandomAccessFile logFile = new RandomAccessFile(file, "r");
    try {
      File metaDataFile = Serialization.getMetaDataFile(file);
      // either this is a rr for a just created file or
      // the metadata file exists and as such it's V3
      if (logFile.length() == 0L || metaDataFile.exists()) {
        return new LogFileV3.RandomReader(file, encryptionKeyProvider,
            fsyncPerTransaction);
      }
      int version = logFile.readInt();
      if (Serialization.VERSION_2 == version) {
        return new LogFileV2.RandomReader(file);
      }
      throw new IOException("File " + file + " has bad version " +
          Integer.toHexString(version));
    } finally {
      if (logFile != null) {
        try {
          logFile.close();
        } catch (IOException e) {
          LOGGER.warn("Unable to close " + file, e);
        }
      }
    }
  }

  static LogFile.SequentialReader getSequentialReader(File file,
                                                      @Nullable KeyProvider encryptionKeyProvider,
                                                      boolean fsyncPerTransaction)
      throws IOException {
    RandomAccessFile logFile = null;
    try {
      // log日志的meta文件，如log-1.meta
      File metaDataFile = Serialization.getMetaDataFile(file);
      // log日志的old meta文件，如log-1.meta.old
      File oldMetadataFile = Serialization.getOldMetaDataFile(file);
      // log日志的tmp meta文件，如log-1.meta.tmp
      File tempMetadataFile = Serialization.getMetaDataTempFile(file);
      boolean hasMeta = false;
      // FLUME-1699:
      // If the platform does not support atomic rename, then we
      // renamed log.meta -> log.meta.old followed by log.meta.tmp -> log.meta
      // I am not sure if all platforms maintain file integrity during
      // file metadata update operations. So:
      // 1. check if meta file exists
      // 2. If 1 returns false, check if temp exists
      // 3. if 2 is also false (maybe the machine died during temp->meta,
      //    then check if old exists.
      // In the above, we assume that if a file exists, it's integrity is ok.
      // 检查.meta文件是否存在
      if (metaDataFile.exists()) { // 存在.meta文件
        hasMeta = true;
      } else if (tempMetadataFile.exists()) { // .meta不存在，检查.meta.tmp是否存在
        if (tempMetadataFile.renameTo(metaDataFile)) { // 将.meta.tmp -> .meta
          hasMeta = true;
        } else {
          throw new IOException("Renaming of " + tempMetadataFile.getName()
              + " to " + metaDataFile.getName() + " failed");
        }
      } else if (oldMetadataFile.exists()) { // .meta和.meta.tmp都不存在，检查.meta.old是否存在
        if (oldMetadataFile.renameTo(metaDataFile)) { // 将.meta.old -> .meta
          hasMeta = true;
        } else {
          throw new IOException("Renaming of " + oldMetadataFile.getName()
              + " to " + metaDataFile.getName() + " failed");
        }
      }

      // 存在.meta文件了
      if (hasMeta) {
        // Now the metadata file has been found, delete old or temp files
        // so it does not interfere with normal operation.
        // 删除.meta.old和.meta.tmp两个文件
        if (oldMetadataFile.exists()) {
          oldMetadataFile.delete();
        }
        if (tempMetadataFile.exists()) {
          tempMetadataFile.delete();
        }

        // 检查文件长度
        if (metaDataFile.length() == 0L) {
          if (file.length() != 0L) {
            // .meta文件长度为0，但log文件长度不为0，说明有问题，抛异常
            String msg = String.format("MetaData file %s is empty, but log %s" +
                " is of size %d", metaDataFile, file, file.length());
            throw new IllegalStateException(msg);
          }

          // .meta文件长度为0，log文件长度也为0，说明读到尾了
          throw new EOFException(String.format("MetaData file %s is empty",
              metaDataFile));
        }

        // 返回SequentialReader
        return new LogFileV3.SequentialReader(file, encryptionKeyProvider,
            fsyncPerTransaction);
      }

      // 不存在.meta文件，则检查是否是V2版本的log文件
      // 构建log文件的RAF
      logFile = new RandomAccessFile(file, "r");
      // 读取log文件的版本号
      int version = logFile.readInt();
      if (Serialization.VERSION_2 == version) {
        // 返回V2版本
        return new LogFileV2.SequentialReader(file);
      }

      // 其他情况，抛异常，如V3版本，但不存在.meta文件
      throw new IOException("File " + file + " has bad version " +
          Integer.toHexString(version));
    } finally {
      if (logFile != null) {
        try {
          logFile.close();
        } catch (IOException e) {
          LOGGER.warn("Unable to close " + file, e);
        }
      }
    }
  }
}
