package com.tfs.flashml.util

/**
  * Log4j doesn't allow to catch stdout and stderr messages out of the box.
  * Found and implemented the solution here
  * https://stackoverflow.com/a/28579006/8169975
  *
  * @since 17/7/18
  */

import java.io._
import org.apache.log4j._

class LoggingOutputStream extends OutputStream {

  /**
    * Default number of bytes in the buffer.
    */
  private val DEFAULT_BUFFER_LENGTH: Int = 2048

  /**
    * Indicates stream state.
    */
  private var hasBeenClosed: Boolean = false

  /**
    * Internal buffer where data is stored.
    */
  private var buf: Array[Byte] = null

  /**
    * The number of valid bytes in the buffer.
    */
  private var count: Int = 0

  /**
    * Remembers the size of the buffer.
    */
  private var curBufLength: Int = 0

  /**
    * The logger to write to.
    */
  private var log: Logger = null

  /**
    * The log level.
    */
  private var level: Level = null

  /**
    * Creates the Logging instance to flush to the given logger.
    *
    * @param log   the Logger to write to
    * @param level the log level
    * @throws IllegalArgumentException in case if one of arguments is  null.
    */
  def this(log: Logger, level: Level)
  {
    this()
    if (log == null || level == null)
      throw new IllegalArgumentException("Logger or log level must be not null")

    this.log = log
    this.level = level
    curBufLength = DEFAULT_BUFFER_LENGTH
    buf = new Array[Byte](curBufLength)
    count = 0
  }

  /**
    * Writes the specified byte to this output stream.
    *
    * @param b the byte to write
    * @throws IOException if an I/O error occurs.
    */
  def write(b: Int): Unit = {
    if (hasBeenClosed) throw new IOException("The stream has been closed.")

    // don't log nulls
    if (b == 0) return


    if (count == curBufLength) {
      // grow the buffer
      val newBufLength = curBufLength + DEFAULT_BUFFER_LENGTH
      val newBuf = new Array[Byte](newBufLength)
      System.arraycopy(buf, 0, newBuf, 0, curBufLength)
      buf = newBuf
      curBufLength = newBufLength
    }

    buf(count) = b.toByte
    count += 1
  }

  /**
    * Flushes this output stream and forces any buffered output
    * bytes to be written out.
    */
  override def flush(): Unit = {
    if (count == 0) return
    val bytes = new Array[Byte](count)
    System.arraycopy(buf, 0, bytes, 0, count)
    val str = new String(bytes)
    log.log(level, str)
    count = 0
  }

  /**
    * Closes this output stream and releases any system resources
    * associated with this stream.
    */
  override def close(): Unit = {
    flush()
    hasBeenClosed = true
  }

}
