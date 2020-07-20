package com.tfs.flashml.systemTests

import com.tfs.flashml.core.PipelineSteps
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
import com.tfs.flashml.util.conf.FlashMLConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

class TotalVisitorsCountTest extends AnyFlatSpec {

  private val log = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("breeze").setLevel(Level.OFF)

  //Load application.conf
  val configSolutionsKeyPair: Config = ConfigFactory.load()

  log.info("Starting FlashML test application")
  println("Test Case: Total Visitors count")

  FlashMLConfig.config = ConfigFactory.load("totalVisitorsCount_test_config.json")

  val appName = s"${FlashMLConfig.getString(FlashMLConstants.FLASHML_PROJECT_ID)}/${FlashMLConfig.getString(FlashMLConstants.FLASHML_MODEL_ID)}/${FlashMLConfig.getString(FlashMLConstants.FLASHML_JOB_ID)}"
  val context = FlashMLConfig.getString(FlashMLConstants.CONTEXT)
  val HIVE_METASTORE_KEY = "hive.metastore.uris"
  val HIVE_METASTORE_THRIFT_URL = FlashMLConfig.getString(FlashMLConstants.HIVE_THRIFT_URL)

  val ss = SparkSession.builder()
    .config(HIVE_METASTORE_KEY, HIVE_METASTORE_THRIFT_URL)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "com.tfs.flashml.util.FlashMLKryoRegistrator")
    .config("spark.extraListeners","com.tfs.flashml.util.CustomSparkListener")
    .config("spark.kryoserializer.buffer.max", "256")
    .config("spark.sql.parquet.compression.codec", "gzip")
    .config("spark.ui.showConsoleProgress", "False")
    .master(context)
    .appName(appName)
    .enableHiveSupport().getOrCreate()
  PipelineSteps.run()

  var vectorizedDf: DataFrame =  SavePointManager.loadData(FlashMLConstants.VECTORIZATION)(0)
    .withColumn("visitors", concat(ConfigValues.primaryKeyColumns.map(col): _*))

  var vectorizedTotalVisitors: Long = vectorizedDf.select("visitors").distinct().count

  var predictDf: DataFrame = SavePointManager.loadData(FlashMLConstants.SCORING)(0)
    .withColumn("visitors", concat(ConfigValues.primaryKeyColumns.map(col): _*))

  val predictTotalVisitors: Long = predictDf.select("visitors").distinct().count()

  "Total Visitors post vectorized and post predict" should "match" in {
    withClue("Total visitors: ") {
      assert(vectorizedTotalVisitors == predictTotalVisitors)
    }
  }

}
