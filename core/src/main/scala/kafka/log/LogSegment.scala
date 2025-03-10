/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log

import kafka.message._
import kafka.common._
import kafka.utils._
import kafka.server.{LogOffsetMetadata, FetchDataInfo}

import scala.math._
import java.io.File


 /**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each 
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 * 
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file. 
 * 
 * @param log The message set containing log entries
 * @param index The offset index
 * @param baseOffset A lower bound on the offsets in this segment
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 * @param time The time instance
 */
@nonthreadsafe
class LogSegment(val log: FileMessageSet, 
                 val index: OffsetIndex, 
                 val baseOffset: Long, 
                 val indexIntervalBytes: Int,
                 val rollJitterMs: Long,
                 time: Time) extends Logging {
  
  var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index */
  private var bytesSinceLastIndexEntry = 0

  def this(dir: File, startOffset: Long, indexIntervalBytes: Int, maxIndexSize: Int, rollJitterMs: Long, time: Time) =
    this(new FileMessageSet(file = Log.logFilename(dir, startOffset)), 
         new OffsetIndex(file = Log.indexFilename(dir, startOffset), baseOffset = startOffset, maxIndexSize = maxIndexSize),
         startOffset,
         indexIntervalBytes,
         rollJitterMs,
         time)
    
  /* Return the size in bytes of this log segment */
  def size: Long = log.sizeInBytes()
  
  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   * 
   * It is assumed this method is being called from within a lock.
   * 
   * @param offset The first offset in the message set.
   * @param messages The messages to append.
   */
  @nonthreadsafe
  def append(offset: Long, messages: ByteBufferMessageSet) {
    if (messages.sizeInBytes > 0) {
      trace("Inserting %d bytes at offset %d at position %d".format(messages.sizeInBytes, offset, log.sizeInBytes()))
      // append an entry to the index (if needed)
      if(bytesSinceLastIndexEntry > indexIntervalBytes) {
        index.append(offset, log.sizeInBytes())
        this.bytesSinceLastIndexEntry = 0
      }
      // append the messages
      log.append(messages)
      this.bytesSinceLastIndexEntry += messages.sizeInBytes
    }
  }
  
  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   * 
   * The lowerBound argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   * 
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   * 
   * @return The position in the log storing the message with the least offset >= the requested offset or null if no message meets this criteria.
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): OffsetPosition = {
    // 通过offset在 OffsetIndex 中查找第一条 offset >= targetOffset 的消息。
    val mapping = index.lookup(offset)
    // 从 startingFilePosition 指定的文件位置开始搜索，查找某条消息的物理位置，该消息是最后一条 offset >= targetOffset 的消息，并返回该消息的 OffsetPosition
    log.searchFor(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * 从 LogSegment 读取第一条满足 offset >= startOffset 的消息。
   * 消息不会超过 maxSize 指定的 bytes，并且会在 maxOffset 之前结束（如果指定了 maxOffset 的话）
   *
   * @param startOffset A lower bound on the first offset to include in the message set we read
   * @param maxSize The maximum number of bytes to include in the message set we read
   * @param maxOffset An optional maximum offset for the message set we read
   * 
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   */
  @threadsafe
  def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int): FetchDataInfo = {
    if(maxSize < 0)
      throw new IllegalArgumentException("Invalid max size for log read (%d)".format(maxSize))

    val logSize = log.sizeInBytes() // this may change, need to save a consistent copy
    // 通过查找 OffsetIndex 来查找 OffsetPosition，这里包含了消息的 offset 以及在对应的 LogSegment 的字节偏移量
    val startPosition = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    if(startPosition == null)
      return null

    // LogOffsetMetadata 包含了消息的 offset，LogSegment 的 baseOffset，以及在 LogSegment 的字节偏移量
    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition.position)

    // if the size is zero, still return a log segment but with zero size
    if(maxSize == 0)
      return FetchDataInfo(offsetMetadata, MessageSet.Empty)

    // 计算可以读取的最大长度，这个由 maxOffset，maxSize 共同限定。
    // 例如，我们开启了某些策略的时候，虽然部分日志已经写入到 LogSegment，但是仍然不允许他们被消费，这个时候我们需要对 maxOffset 进行限制。
    // 如果没有指定 maxOffset，那么我们可以直接使用 maxSize 作为 length
    // 如果指定了 maxOffset，那么我们需要找到 maxOffset 对应的日志的 OffsetPosition
    // 那么进而可以计算得到我们需要读取的数据长度了。
    val length =
      maxOffset match {
        case None =>
          maxSize
        case Some(offset) => {
          if(offset < startOffset)
            throw new IllegalArgumentException("Attempt to read with a maximum offset (%d) less than the start offset (%d).".format(offset, startOffset))
          val mapping = translateOffset(offset, startPosition.position)
          val endPosition =
            if(mapping == null)
              logSize // the max offset is off the end of the log, use the end of the file
            else
              mapping.position
          min(endPosition - startPosition.position, maxSize)
        }
      }
    FetchDataInfo(offsetMetadata, log.read(startPosition.position, length))
  }
  
  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes from the end of the log and index.
   * 
   * @param maxMessageSize A bound the memory allocation in the case of a corrupt message size--we will assume any message larger than this
   * is corrupt.
   * 
   * @return The number of bytes truncated from the log
   */
  @nonthreadsafe
  def recover(maxMessageSize: Int): Int = {
    index.truncate()
    index.resize(index.maxIndexSize)
    var validBytes = 0
    var lastIndexEntry = 0
    val iter = log.iterator(maxMessageSize)
    try {
      while(iter.hasNext) {
        val entry = iter.next
        entry.message.ensureValid()
        if(validBytes - lastIndexEntry > indexIntervalBytes) {
          // we need to decompress the message, if required, to get the offset of the first uncompressed message
          val startOffset =
            entry.message.compressionCodec match {
              case NoCompressionCodec =>
                entry.offset
              case _ =>
                ByteBufferMessageSet.decompress(entry.message).head.offset
          }
          index.append(startOffset, validBytes)
          lastIndexEntry = validBytes
        }
        validBytes += MessageSet.entrySize(entry.message)
      }
    } catch {
      case e: InvalidMessageException => 
        logger.warn("Found invalid messages in log segment %s at byte offset %d: %s.".format(log.file.getAbsolutePath, validBytes, e.getMessage))
    }
    val truncated = log.sizeInBytes - validBytes
    log.truncateTo(validBytes)
    index.trimToValidSize()
    truncated
  }

  override def toString() = "LogSegment(baseOffset=" + baseOffset + ", size=" + size + ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    val mapping = translateOffset(offset)
    if(mapping == null)
      return 0
    index.truncateTo(offset)
    // after truncation, reset and allocate more space for the (new currently  active) index
    index.resize(index.maxIndexSize)
    val bytesTruncated = log.truncateTo(mapping.position)
    if(log.sizeInBytes == 0)
      created = time.milliseconds
    bytesSinceLastIndexEntry = 0
    bytesTruncated
  }
  
  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def nextOffset(): Long = {
    val ms = read(index.lastOffset, None, log.sizeInBytes)
    if(ms == null) {
      baseOffset
    } else {
      ms.messageSet.lastOption match {
        case None => baseOffset
        case Some(last) => last.nextOffset
      }
    }
  }
  
  /**
   * Flush this log segment to disk
   */
  @threadsafe
  def flush() {
    LogFlushStats.logFlushTimer.time {
      log.flush()
      index.flush()
    }
  }
  
  /**
   * Change the suffix for the index and log file for this log segment
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String) {
    val logRenamed = log.renameTo(new File(Utils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    if(!logRenamed)
      throw new KafkaStorageException("Failed to change the log file suffix from %s to %s for log segment %d".format(oldSuffix, newSuffix, baseOffset))
    val indexRenamed = index.renameTo(new File(Utils.replaceSuffix(index.file.getPath, oldSuffix, newSuffix)))
    if(!indexRenamed)
      throw new KafkaStorageException("Failed to change the index file suffix from %s to %s for log segment %d".format(oldSuffix, newSuffix, baseOffset))
  }
  
  /**
   * Close this log segment
   */
  def close() {
    Utils.swallow(index.close)
    Utils.swallow(log.close)
  }
  
  /**
   * Delete this log segment from the filesystem.
   * @throws KafkaStorageException if the delete fails.
   */
  def delete() {
    val deletedLog = log.delete()
    val deletedIndex = index.delete()
    if(!deletedLog && log.file.exists)
      throw new KafkaStorageException("Delete of log " + log.file.getName + " failed.")
    if(!deletedIndex && index.file.exists)
      throw new KafkaStorageException("Delete of index " + index.file.getName + " failed.")
  }
  
  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified
  
  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    log.file.setLastModified(ms)
    index.file.setLastModified(ms)
  }
}