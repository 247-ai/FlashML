package com.tfs.flashml.systemTests

import com.tfs.flashml.core.PipelineSteps
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util._
import com.tfs.flashml.util.conf.FlashMLConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.scalatest._
import org.slf4j.LoggerFactory

class MultiIntentLRStratifiedSamplingTest  extends FlatSpec{

  private val log = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("breeze").setLevel(Level.OFF)

  //Load application.conf
  val configSolutionsKeyPair: Config = ConfigFactory.load()

  println("=============================================================================================")
  log.info("Starting FlashML test application")
  println("Test Case: LR Multiple Intent Stratified Sampling")

  FlashMLConfig.config = ConfigFactory.load("multiIntent_lr_stratifiedSampling_test_config.json")

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

  import ss.implicits._

  var multiIntentLRPredictionDF: Array[RDD[(Double,Double)]] = SavePointManager.loadData(FlashMLConstants.SCORING).map(_.select("prediction", ConfigUtils.getIndexedResponseColumn).as[(Double, Double)].rdd)

  val multiIntentLREvaluatorTrain = new MulticlassMetrics(multiIntentLRPredictionDF(0))

  "LR-MultiIntent-Train-Precision" should "match" in {
    withClue("LR-MultiIntent-Train-Precision: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentLRStratified.trainPrecision").toDouble) {
        multiIntentLREvaluatorTrain.weightedPrecision
      }
    }
  }

  "LR-MultiIntent-Train-Recall" should "match" in {
    withClue("LR-MultiIntent-Train-Recall: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentLRStratified.trainRecall").toDouble) {
        multiIntentLREvaluatorTrain.weightedRecall
      }
    }
  }

  val multiIntentLREvaluatorTest = new MulticlassMetrics(multiIntentLRPredictionDF(1))

  "LR-MultiIntent-Test-Precision" should "match" in {
    withClue("LR-MultiIntent-Test-Precision: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentLRStratified.testPrecision").toDouble) {
        multiIntentLREvaluatorTest.weightedPrecision
      }
    }
  }

  "LR-MultiIntent-Test-Recall" should "match" in {
    withClue("LR-MultiIntent-Test-Recall: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentLRStratified.testRecall").toDouble) {
        multiIntentLREvaluatorTest.weightedRecall
      }
    }
  }
}
