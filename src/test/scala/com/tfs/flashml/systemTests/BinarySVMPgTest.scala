package com.tfs.flashml.systemTests

import com.tfs.flashml.core.PipelineSteps
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util._
import com.tfs.flashml.util.conf.FlashMLConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.scalatest._
import org.slf4j.LoggerFactory

class BinarySVMPgTest extends FlatSpec {

  private val log = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("breeze").setLevel(Level.OFF)

  //Load application.conf
  val configSolutionsKeyPair: Config = ConfigFactory.load()

  println("=============================================================================================")
  log.info("Starting FlashML test application")
  println("Test Case: SVM Single Intent Page Level")

  FlashMLConfig.config = ConfigFactory.load("singleIntent_svm_PG_test_config.json")

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

  var svmSIPGPredictionDF: Array[RDD[(Double,Double)]] = SavePointManager.loadData(FlashMLConstants.SCORING).map(_.withColumn("positive_probability", ConfigUtils.pos_prob(col("probability"))).select("positive_probability", ConfigUtils.getIndexedResponseColumn).as[(Double, Double)].rdd)

  val svmSIPGEvaluator1 = new BinaryClassificationMetrics(svmSIPGPredictionDF(0))
  "SVM-SingleIntent-PG-TrainAUC page1" should "match" in {
    withClue("SVM-SingleIntent-PG-TrainAUC page1: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPG.trainAUROCpage1").toDouble) {
        svmSIPGEvaluator1.areaUnderROC()
      }
    }
  }

  val svmSIPGEvaluator2 = new BinaryClassificationMetrics(svmSIPGPredictionDF(1))
  "SVM-SingleIntent-PG-TrainAUC page2" should "match" in {
    withClue("SVM-SingleIntent-PG-TrainAUC page2: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPG.trainAUROCpage2").toDouble) {
        svmSIPGEvaluator2.areaUnderROC()
      }
    }
  }

  val svmSIPGEvaluator3 = new BinaryClassificationMetrics(svmSIPGPredictionDF(2))
  "SVM-SingleIntent-PG-TrainAUC page3" should "match" in {
    withClue("SVM-SingleIntent-PG-TrainAUC page3: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPG.trainAUROCpage3").toDouble) {
        svmSIPGEvaluator3.areaUnderROC()
      }
    }
  }

  val svmSIPGEvaluator4 = new BinaryClassificationMetrics(svmSIPGPredictionDF(3))
  "SVM-SingleIntent-PG-TrainAUC page4" should "match" in {
    withClue("SVM-SingleIntent-PG-TrainAUC page4: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPG.trainAUROCpage4").toDouble) {
        svmSIPGEvaluator4.areaUnderROC()
      }
    }
  }

  val svmSIPGEvaluator5 = new BinaryClassificationMetrics(svmSIPGPredictionDF(4))
  "SVM-SingleIntent-PG-TestAUC page1" should "match" in {
    withClue("SVM-SingleIntent-PG-TestAUC page1: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPG.testAUROCpage1").toDouble) {
        svmSIPGEvaluator5.areaUnderROC()
      }
    }
  }

  val svmSIPGEvaluator6 = new BinaryClassificationMetrics(svmSIPGPredictionDF(5))
  "SVM-SingleIntent-PG-TestAUC page2" should "match" in {
    withClue("SVM-SingleIntent-PG-TestAUC page2: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPG.testAUROCpage2").toDouble) {
        svmSIPGEvaluator6.areaUnderROC()
      }
    }
  }

  val svmSIPGEvaluator7 = new BinaryClassificationMetrics(svmSIPGPredictionDF(6))
  "SVM-SingleIntent-PG-TestAUC page3" should "match" in {
    withClue("SVM-SingleIntent-PG-TestAUC page3: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPG.testAUROCpage3").toDouble) {
        svmSIPGEvaluator7.areaUnderROC()
      }
    }
  }

  val svmSIPGEvaluator8 = new BinaryClassificationMetrics(svmSIPGPredictionDF(7))
  "SVM-SingleIntent-PG-TestAUC page4" should "match" in {
    withClue("SVM-SingleIntent-PG-TestAUC page4: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPG.testAUROCpage4").toDouble) {
        svmSIPGEvaluator8.areaUnderROC()
      }
    }
  }

}
