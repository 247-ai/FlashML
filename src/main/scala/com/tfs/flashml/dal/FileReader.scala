package com.tfs.flashml.dal

import com.tfs.flashml.core.DirectoryCreator
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.JavaConversions._
import java.util.Map

import scala.collection.immutable.HashMap

/**
 * Class for reading input file from HDFS.
 *
 * @since 8/11/16.
 */
class FileReader(protocolPrefix: String, inputFileName: String) extends DataReader
{
    private val log = LoggerFactory.getLogger(getClass)

    override def read: DataFrame =
    {
        // Determine input file path
        val inputFilePath: String = s"${FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI)}/$inputFileName"

        val projDataHashMap: mutable.Map[String, Any] = FlashMLConfig
                .config
                .getAnyRef(FlashMLConstants.INPUT_PATH)
                .asInstanceOf[java.util.HashMap[String, Any]]
                .asScala

        // Load data
        val df: DataFrame = projDataHashMap("format").asInstanceOf[String].toLowerCase match
        {
            case "csv" => SparkSession.builder.getOrCreate().read.option("header", "true").csv(inputFilePath)
            case "tsv" => SparkSession.builder.getOrCreate().read.option("sep", "\t").option("header", "true").csv(inputFilePath)
            case "json" => SparkSession
                    .builder
                    .getOrCreate()
                    .read
                    .json(inputFilePath)
            case _ => throw new Exception("Supported parameter formats are: csv, tsv and json")
        }

        log.info(s"Loaded training data from [$inputFilePath]")

        // Registering data as temp table
        df.createOrReplaceTempView(inputFileName.split("/").last.split("\\.")(0))
        val prefix = projDataHashMap("temp_table_prefix").asInstanceOf[String]
        val queries = projDataHashMap("queries")
                .asInstanceOf[java.util.ArrayList[String]]
                .asScala

        // Applying queries
        val queryDFs = if (queries.nonEmpty) processSQLViewsRec(queries, 1, prefix, SparkSession.builder.getOrCreate())
        else df

        // Apply remaining filters etc
        val inputDF = processInputData(queryDFs)

        inputDF
    }
}
