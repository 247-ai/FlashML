package com.tfs.flashml.systemTests

import com.tfs.flashml.core.PipelineSteps
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

class BinaryGBTHyperband extends AnyFlatSpec {

    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)

    //Load application.conf
    val configSolutionsKeyPair: Config = ConfigFactory.load()

    FlashMLConfig.config= ConfigFactory.load("binary_gbt_hyperband.json")

    val appName = s"${FlashMLConfig.getString(FlashMLConstants.FLASHML_PROJECT_ID)}/${FlashMLConfig.getString(FlashMLConstants.FLASHML_MODEL_ID)}/${FlashMLConfig.getString(FlashMLConstants.FLASHML_JOB_ID)}"
    val context = FlashMLConfig.getString(FlashMLConstants.CONTEXT)
    val HIVE_METASTORE_KEY = "hive.metastore.uris"
    val HIVE_METASTORE_THRIFT_URL = FlashMLConfig.getString(FlashMLConstants.HIVE_THRIFT_URL)

    val spark = SparkSession.builder()
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

    println("=============================================================================================")
    log.info("Starting FlashML test application")
    println("Test case: Binary Gradient Boosted Trees with Hyperband")

    PipelineSteps.run()

    var binaryPrediction: Array[DataFrame] = SavePointManager.loadData(FlashMLConstants.SCORING)
    val binaryEvaluator: BinaryClassificationEvaluator = new BinaryClassificationEvaluator()
            .setLabelCol(ConfigValues.getIndexedResponseColumn)

    "The Binary GB Trees TrainAUROC" should "match" in {
        withClue("GBT Trees TrainAUROC: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.binaryGBTHyperband.trainAUROC").toDouble) {
                binaryEvaluator.setMetricName("areaUnderROC").evaluate(binaryPrediction(0))
            }
        }
    }

    "The Binary GB Trees TestAUROC" should "match" in {
        withClue("GBT Trees TestAUROC: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.binaryGBTHyperband.testAUROC").toDouble) {
                binaryEvaluator.setMetricName("areaUnderROC").evaluate(binaryPrediction(1))
            }
        }
    }

}
