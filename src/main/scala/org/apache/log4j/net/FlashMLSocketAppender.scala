package org.apache.log4j.net

import java.io.{IOException, InterruptedIOException}
import com.tfs.flashml.util.FlashMLConfig
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.log4j.helpers.LogLog
import org.apache.log4j.spi.LoggingEvent

/**
  * Class for aggregating Spark logs.
  * @since 19/8/18
  */
class FlashMLSocketAppender extends SocketAppender {

  override def append(event: LoggingEvent): Unit = {

    if (event != null) {
      if (this.address == null)
        this.errorHandler.error("No remote host is set for SocketAppender named \"" + this.name + "\".")
      else if (this.oos != null) {
        try {
          if (this.locationInfo) event.getLocationInformation

          event.getNDC
          event.getThreadName
          event.getMDCCopy()

          //Added property
          event.setProperty("jobId", FlashMLConfig.getString(FlashMLConstants.FLASHML_JOB_ID))

          event.getRenderedMessage
          event.getThrowableStrRep

          this.oos.writeObject(event)
          this.oos.flush()

          if ( {
            this.counter += 1
            this.counter
          } >= 1) {
            this.counter = 0
            this.oos.reset()
          }
        } catch {
          case ex: IOException =>
            if (ex.isInstanceOf[InterruptedIOException]) Thread.currentThread.interrupt()

            this.oos = null
            LogLog.warn("Detected problem with connection: " + ex)

            if (this.reconnectionDelay > 0) this.fireConnector()
            else this.errorHandler.error("Detected problem with connection, not reconnecting.", ex, 0)
        }

      }
    }
  }
}
