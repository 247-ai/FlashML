package com.tfs.flashml.systemTests

import com.tfs.flashml.core.PipelineSteps
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

class BinarySVMTest extends AnyFlatSpec {

  private val log = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("breeze").setLevel(Level.OFF)

  //Load application.conf
  val configSolutionsKeyPair: Config = ConfigFactory.load()

  println("=============================================================================================")
  println("Test Case: SVM Single Intent")

  FlashMLConfig.config = ConfigFactory.load("singleIntent_svm_test_config.json")

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

  var svmSIPredictionDF: Array[DataFrame] = SavePointManager.loadData(FlashMLConstants.SCORING)
  val svmSIEvaluator: BinaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol(ConfigUtils.getIndexedResponseColumn)

  "SVM-SingleIntent-TrainAUC" should "match" in {
    withClue("SVM-SingleIntent-TrainAUC: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntent.trainAUROC").toDouble) {
        svmSIEvaluator.setMetricName("areaUnderROC").evaluate(svmSIPredictionDF(0))
      }
    }
  }

  "SVM-SingleIntent-TestAUC" should "match" in {
    withClue("SVM-SingleIntent-TestAUC: ") {
      assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntent.testAUROC").toDouble) {
        svmSIEvaluator.setMetricName("areaUnderROC").evaluate(svmSIPredictionDF(1))
      }
    }
  }

}
