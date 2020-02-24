package com.tfs.flashml.dal

import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.spark.SparkException
import org.slf4j.LoggerFactory
import scala.util.matching.Regex

/**
 * Factory class to read input data.
 *
 * @since 7/12/16.
 */
object DataReaderFactory
{

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Example Usage :  val inputRegex = "(hive|hdfs|simod)://(.*)".r
   * val inputRegex(prefix, suffix) = "hive//localhost:1000/foo"
   *
   *
   * prefix: String = hive
   * suffix: String = localhost:1000/foo
   *
   */

  val HIVE = "hive"
  val HDFS = "hdfs"
  val VERTICA = "vertica"

  val inputRegex: Regex = "^(.*)://(.*)".r


  def get(): DataReader =
  {

    val source: String = FlashMLConfig.getString(FlashMLConstants.INPUT_PATH + ".source")

    val inputRegex(prefix, suffix) = source
    prefix.toLowerCase match
    {
      case HIVE => new HiveReader(suffix)
      case VERTICA => new VerticaReader(suffix)
      case HDFS => new FileReader(prefix.toLowerCase, suffix)
      case _ => throwException("Input Data Source should be hive (hive://), vertica (vertica://), or hdfs " +
        "(hdfsfile:// or hdfs://)")
    }
  }

  private def throwException(msg: String) =
  {
    log.error(msg)
    throw new SparkException(msg)
  }

}
