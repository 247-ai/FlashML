package com.tfs.flashml.dal

import java.util.Properties

import scala.collection.JavaConverters._
import com.tfs.flashml.util.{FlashMLConfig}
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Class for reading input data from Vertica.
 *
 * @since 27/12/16.
 */
class VerticaReader(suffix: String) extends DataReader
{
  private val log = LoggerFactory.getLogger(getClass)

  private val properties = new Properties()
  properties.setProperty("user", FlashMLConfig.getString(FlashMLConstants.VERTICA_USER_NAME))
  properties.setProperty("password", FlashMLConfig.getString(FlashMLConstants.VERTICA_USER_PASSWORD))
  properties.setProperty("driver", FlashMLConfig.getString(FlashMLConstants.VERTICA_JDBC_DRIVER))

  override def read: DataFrame =
  {

    val df: DataFrame = if (suffix.nonEmpty)
    {
      SparkSession.builder.getOrCreate().read.jdbc(FlashMLConfig.getString(FlashMLConstants.VERTICA_HOST_URL),
        suffix, properties)
    }
    else
    {
      // Parse Queries
      // NOTE Assumes only first query is jdbc
      val projDataHashMap = FlashMLConfig
        .config
        .getAnyRef(FlashMLConstants.INPUT_PATH)
        .asInstanceOf[java.util.HashMap[String, Any]]

      val prefix = projDataHashMap.get("temp_table_prefix").toString

      val queries = projDataHashMap
        .get("queries")
        .asInstanceOf[java.util.ArrayList[String]]
        .asScala

      // For vertica self process the first query
      val firstQuery = " ( " + queries.head + " ) first_table "
      val df = SparkSession.builder.getOrCreate().read.jdbc(FlashMLConfig.getString(FlashMLConstants
        .VERTICA_HOST_URL), firstQuery, properties)
      df.createOrReplaceTempView(prefix + 1.toString)

      val queryDFs = if (queries.nonEmpty) processSQLViewsRec(queries.tail, 2, prefix, SparkSession.builder
        .getOrCreate())

      else df
      queryDFs

    }

    // Apply remaining filters etc
    val inputDF = processInputData(df)
    inputDF
  }
}