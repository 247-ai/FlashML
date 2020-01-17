package com.tfs.flashml.systemTests

import com.tfs.flashml.core.PipelineSteps
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import com.tfs.flashml.util.conf.FlashMLConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class MultiIntentNBCVTest extends FlatSpec{

    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)

    //Load application.conf
    val configSolutionsKeyPair: Config = ConfigFactory.load()

    FlashMLConfig.config= ConfigFactory.load("multiIntent_nb_cv_test_config.json")

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
    println("Test case: MultiIntent NB with CV")

    PipelineSteps.run()

    import spark.implicits._

    var multiIntentNBCVPredictionDF: Array[RDD[(Double,Double)]] = SavePointManager
            .loadData(FlashMLConstants.SCORING)
            .map(_.select("prediction", ConfigUtils.getIndexedResponseColumn)
                    .as[(Double, Double)]
                    .rdd)


    val multiIntentNBCVEvaluatorTrain = new MulticlassMetrics(multiIntentNBCVPredictionDF(0))

    "NB - MultiIntent-CV-Train-Precision" should "match" in {
        withClue("NB -MultiIntent-Train-Precision: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentNBCV.trainPrecision").toDouble) { multiIntentNBCVEvaluatorTrain.weightedPrecision
            }
        }
    }

    "NB -MultiIntent-CV-Train-Recall" should "match" in {
        withClue("NB -MultiIntent-Train-Recall: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentNBCV.trainRecall").toDouble) {
                multiIntentNBCVEvaluatorTrain.weightedRecall
            }

        }
    }

    val multiIntentNBCVEvaluatorTest = new MulticlassMetrics(multiIntentNBCVPredictionDF(1))

    "NB -MultiIntent-CV-Test-Precision" should "match" in {
        withClue("NB -MultiIntent-Test-Precision: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentNBCV.testPrecision").toDouble) {
                multiIntentNBCVEvaluatorTest.weightedPrecision
            }

        }
    }

    "NB -MultiIntent-CV-Test-Recall" should "match" in {
        withClue("NB -MultiIntent-Test-Recall: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentNBCV.testRecall").toDouble) { multiIntentNBCVEvaluatorTest.weightedRecall
            }
        }
    }

}
