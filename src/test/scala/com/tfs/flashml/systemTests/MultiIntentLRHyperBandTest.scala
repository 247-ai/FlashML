package com.tfs.flashml.systemTests

import com.tfs.flashml.core.PipelineSteps
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

class MultiIntentLRHyperBandTest extends AnyFlatSpec
{
    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    //Load application.conf
    val configSolutionsKeyPair: Config = ConfigFactory.load()

    FlashMLConfig.config= ConfigFactory.load("multiIntent_lr_hyperband_test_config.json")

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

    //Test cases for binary logistic regression
    println("=============================================================================================")
    log.info("Starting FlashML test application")
    println("Test case: MultiIntent SVM with Hyperband")

    //FlashMLConfig.config = ConfigFactory.load("binary_test_config.properties")

    PipelineSteps.run()

    import spark.implicits._

    var multiIntentLRPredictionDF: Array[RDD[(Double,Double)]] = SavePointManager.loadData(FlashMLConstants.SCORING).map(_.select("prediction", ConfigUtils.getIndexedResponseColumn).as[(Double, Double)].rdd)

    val multiIntentLREvaluatorTrain = new MulticlassMetrics(multiIntentLRPredictionDF(0))
    println("Train accuracy: "+ multiIntentLREvaluatorTrain.accuracy)
    println("Train precision: " + multiIntentLREvaluatorTrain.weightedPrecision)
    println("Train recall: " + multiIntentLREvaluatorTrain.weightedRecall)
    val multiIntentLREvaluatorTest = new MulticlassMetrics(multiIntentLRPredictionDF(1))
    println("Test accuracy: "+ multiIntentLREvaluatorTest.accuracy)
    println("Test precision: " + multiIntentLREvaluatorTest.weightedPrecision)
    println("Test recall: " + multiIntentLREvaluatorTest.weightedRecall)

    "LR-MultiIntent-Hyperband-Prediction-Dataframe-Size" should "match" in {
        withClue("LR-MultiIntent-Train-Precision: ") {
                assertResult(2){ multiIntentLRPredictionDF.length
            }
        }
    }

    "LR-MultiIntent-Hyperband-Train-Precision" should "match" in {
        withClue("LR-MultiIntent-Train-Precision: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.binaryLRHyperband.trainPrecision").toDouble) { multiIntentLREvaluatorTrain.weightedPrecision
            }
        }
    }

    "LR-MultiIntent-Hyperband-Train-Recall" should "match" in {
        withClue("LR-MultiIntent-Train-Recall: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.binaryLRHyperband.trainRecall").toDouble) { multiIntentLREvaluatorTrain.weightedRecall
            }
        }
    }

    "LR-MultiIntent-Hyperband-Test-Precision" should "match" in {
        withClue("LR-MultiIntent-Train-Precision: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.binaryLRHyperband.testPrecision").toDouble) { multiIntentLREvaluatorTest.weightedPrecision
            }
        }
    }

    "LR-MultiIntent-Hyperband-Test-Recall" should "match" in {
        withClue("LR-MultiIntent-Test-Recall: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.binaryLRHyperband.testRecall").toDouble) { multiIntentLREvaluatorTest.weightedRecall
            }
        }
    }

}
