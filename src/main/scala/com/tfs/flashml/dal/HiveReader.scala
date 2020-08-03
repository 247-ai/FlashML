package com.tfs.flashml.dal

import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Class for reading input data from Hive.
 *
 * @since 21/11/16.
 */
class HiveReader(suffix: String) extends DataReader
{
    private val log = LoggerFactory.getLogger(getClass)

    override def read: DataFrame =
    {
        // Check if standard df or query based
        val df: DataFrame = if (suffix.nonEmpty)
        {
            // Direct Table loaded from Hive
            SparkSession.builder.getOrCreate().sql("select * from " + suffix)
        }
        else
        {
            // Parse Queries
            val projDataHashMap = FlashMLConfig
                    .config
                    .getAnyRef(FlashMLConstants.INPUT_PATH)
                    .asInstanceOf[java.util.HashMap[String, Any]]
                    .asScala

            val prefix = projDataHashMap("temp_table_prefix")
                    .toString


            val queries = projDataHashMap("queries")
                    .asInstanceOf[java.util.ArrayList[String]]
                    .asScala

            val queryDFs = processSQLViewsRec(queries, 1, prefix, SparkSession.builder.getOrCreate())
            queryDFs
        }

        // Apply remaining filters etc
        val inputDF = processInputData(df)
        inputDF
    }
}
