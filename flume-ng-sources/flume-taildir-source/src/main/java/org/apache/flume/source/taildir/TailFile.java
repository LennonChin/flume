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

package org.apache.flume.source.taildir;

import com.google.common.collect.Lists;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.BYTE_OFFSET_HEADER_KEY;

public class TailFile {
  private static final Logger logger = LoggerFactory.getLogger(TailFile.class);

  private static final byte BYTE_NL = (byte) 10;
  private static final byte BYTE_CR = (byte) 13;

  private static final int BUFFER_SIZE = 8192;
  private static final int NEED_READING = -1;

  private RandomAccessFile raf;
  private final String path;
  private final long inode;
  private long pos;
  private long lastUpdated;
  private boolean needTail;
  private final Map<String, String> headers;
  private byte[] buffer;
  private byte[] oldBuffer;
  private int bufferPos;
  private long lineReadPos;

  public TailFile(File file, Map<String, String> headers, long inode, long pos)
          throws IOException {
    // 创建RandomAccessFile
    this.raf = new RandomAccessFile(file, "r");
    if (pos > 0) {
      // seek到指定Position
      raf.seek(pos);
      // 行读取Position
      lineReadPos = pos;
    }
    // 文件绝对路径
    this.path = file.getAbsolutePath();
    // inode
    this.inode = inode;
    this.pos = pos;
    this.lastUpdated = 0L;
    this.needTail = true;
    this.headers = headers;
    this.oldBuffer = new byte[0];
    this.bufferPos = NEED_READING;
  }

  public RandomAccessFile getRaf() {
    return raf;
  }

  public String getPath() {
    return path;
  }

  public long getInode() {
    return inode;
  }

  // 开始读取的position
  public long getPos() {
    return pos;
  }

  public long getLastUpdated() {
    return lastUpdated;
  }

  public boolean needTail() {
    return needTail;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public long getLineReadPos() {
    return lineReadPos;
  }

  public void setPos(long pos) {
    this.pos = pos;
  }

  public void setLastUpdated(long lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  public void setNeedTail(boolean needTail) {
    this.needTail = needTail;
  }

  public void setLineReadPos(long lineReadPos) {
    this.lineReadPos = lineReadPos;
  }

  public boolean updatePos(String path, long inode, long pos) throws IOException {
    // 检查是否是同一个文件
    if (this.inode == inode && this.path.equals(path)) {
      // 设置pos属性的值
      setPos(pos);
      // 更新文件的position，即使用RandomAccessFile的seek()方法
      updateFilePos(pos);
      logger.info("Updated position, file: " + path + ", inode: " + inode + ", pos: " + pos);
      return true;
    }
    return false;
  }
  public void updateFilePos(long pos) throws IOException {
    raf.seek(pos);
    lineReadPos = pos;
    bufferPos = NEED_READING;
    oldBuffer = new byte[0];
  }


  public List<Event> readEvents(int numEvents, boolean backoffWithoutNL,
                                boolean addByteOffset) throws IOException {
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      // 读取事件
      Event event = readEvent(backoffWithoutNL, addByteOffset);
      if (event == null) {
        break;
      }
      // 存入列表
      events.add(event);
    }
    return events;
  }

  private Event readEvent(boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
    // 暂存文件当前lineReadPos
    Long posTmp = getLineReadPos();
    // 读取一行
    LineResult line = readLine();
    if (line == null) {
      return null;
    }
    if (backoffWithoutNL && !line.lineSepInclude) {
      logger.info("Backing off in file without newline: "
              + path + ", inode: " + inode + ", pos: " + raf.getFilePointer());
      updateFilePos(posTmp);
      return null;
    }
    // 封装为Event
    Event event = EventBuilder.withBody(line.line);
    // 判断是否需要跳过每行的标题
    if (addByteOffset == true) {
      event.getHeaders().put(BYTE_OFFSET_HEADER_KEY, posTmp.toString());
    }
    return event;
  }

  private void readFile() throws IOException {
    if ((raf.length() - raf.getFilePointer()) < BUFFER_SIZE) { // 最大读8K
      // 不够8K
      buffer = new byte[(int) (raf.length() - raf.getFilePointer())];
    } else {
      // 最大读8K
      buffer = new byte[BUFFER_SIZE];
    }
    // 读取数据到buffer
    raf.read(buffer, 0, buffer.length);
    bufferPos = 0;
  }

  private byte[] concatByteArrays(byte[] a, int startIdxA, int lenA,
                                  byte[] b, int startIdxB, int lenB) {
    byte[] c = new byte[lenA + lenB];
    System.arraycopy(a, startIdxA, c, 0, lenA);
    System.arraycopy(b, startIdxB, c, lenA, lenB);
    return c;
  }

  public LineResult readLine() throws IOException {
    LineResult lineResult = null;
    while (true) {
      if (bufferPos == NEED_READING) {
        if (raf.getFilePointer() < raf.length()) { // 不能超出文件的大小
          // 读取文件内容，数据读取到buffer变量中
          readFile();
        } else { // 如果文件的指针已经读到最后了，就判断oldBuffer是否还有数据
          if (oldBuffer.length > 0) {
            // oldBuffer还有数据，包装为lineResult
            lineResult = new LineResult(false, oldBuffer);
            // 将oldBuffer置空
            oldBuffer = new byte[0];
            // 更新TailFile的lineReadPos
            setLineReadPos(lineReadPos + lineResult.line.length);
          }
          // 跳出循环
          break;
        }
      }

      // 走到这里说明buffer已经装有读到的数据了，不过此时oldBuffer也可能装有数据
      for (int i = bufferPos; i < buffer.length; i++) {
        // 读buffer的数据
        if (buffer[i] == BYTE_NL) {
          int oldLen = oldBuffer.length;
          // Don't copy last byte(NEW_LINE)
          int lineLen = i - bufferPos;
          // For windows, check for CR
          // windows系统，检查CR标志，如果存在需要将CR标志去掉
          if (i > 0 && buffer[i - 1] == BYTE_CR) {
            lineLen -= 1;
          } else if (oldBuffer.length > 0 && oldBuffer[oldBuffer.length - 1] == BYTE_CR) {
            oldLen -= 1;
          }

          // 处理oldBuffer的数据
          lineResult = new LineResult(true,
                  concatByteArrays(oldBuffer, 0, oldLen, buffer, bufferPos, lineLen));
          // 更新TailFile的lineReadPos
          setLineReadPos(lineReadPos + (oldBuffer.length + (i - bufferPos + 1)));
          oldBuffer = new byte[0];
          if (i + 1 < buffer.length) {
            bufferPos = i + 1;
          } else {
            bufferPos = NEED_READING;
          }
          break;
        }
      }
      if (lineResult != null) {
        break;
      }
      // NEW_LINE not showed up at the end of the buffer
      // 合并oldBuffer和buffer剩余的数据，赋值该oldBuffer
      oldBuffer = concatByteArrays(oldBuffer, 0, oldBuffer.length,
              buffer, bufferPos, buffer.length - bufferPos);
      bufferPos = NEED_READING;
    }
    return lineResult;
  }

  public void close() {
    try {
      raf.close();
      raf = null;
      long now = System.currentTimeMillis();
      setLastUpdated(now);
    } catch (IOException e) {
      logger.error("Failed closing file: " + path + ", inode: " + inode, e);
    }
  }

  private class LineResult {
    // 标识是否存在换行符
    final boolean lineSepInclude;
    final byte[] line;

    public LineResult(boolean lineSepInclude, byte[] line) {
      super();
      this.lineSepInclude = lineSepInclude;
      this.line = line;
    }
  }
}
