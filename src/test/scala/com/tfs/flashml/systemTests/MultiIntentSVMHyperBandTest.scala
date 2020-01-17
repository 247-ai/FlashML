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
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class MultiIntentSVMHyperBandTest extends FlatSpec
{
    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    //Load application.conf
    val configSolutionsKeyPair: Config = ConfigFactory.load()

    FlashMLConfig.config= ConfigFactory.load("multiIntent_svm_hyperband_test_config.json")

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


    PipelineSteps.run()

    import spark.implicits._

    var multiIntentSVMPredictionDF: Array[RDD[(Double,Double)]] = SavePointManager.loadData(FlashMLConstants.SCORING).map(_.select("prediction", ConfigUtils.getIndexedResponseColumn).as[(Double, Double)].rdd)

    val multiIntentSVMEvaluatorTrain = new MulticlassMetrics(multiIntentSVMPredictionDF(0))
    println("Train accuracy: "+ multiIntentSVMEvaluatorTrain.accuracy)
    println("Train precision: " + multiIntentSVMEvaluatorTrain.weightedPrecision)
    println("Train recall: " + multiIntentSVMEvaluatorTrain.weightedRecall)
    val multiIntentSVMEvaluatorTest = new MulticlassMetrics(multiIntentSVMPredictionDF(1))
    println("Test accuracy: "+ multiIntentSVMEvaluatorTest.accuracy)
    println("Test precision: " + multiIntentSVMEvaluatorTest.weightedPrecision)
    println("Test recall: " + multiIntentSVMEvaluatorTest.weightedRecall)

    "SVM-MultiIntent-Hyperband-Prediction-Dataframe-Size" should "match" in {
        withClue("SVM-MultiIntent-Train-Precision: ") {
            assertResult(2){ multiIntentSVMPredictionDF.length
            }
        }
    }

    "SVM-MultiIntent-Hyperband-Train-Precision" should "match" in {
        withClue("SVM-MultiIntent-Train-Precision: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentSVMHyperband.trainPrecision").toDouble) { multiIntentSVMEvaluatorTrain.weightedPrecision
            }
        }
    }

    "SVM-MultiIntent-Hyperband-Train-Recall" should "match" in {
        withClue("SVM-MultiIntent-Train-Recall: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentSVMHyperband.trainRecall").toDouble) { multiIntentSVMEvaluatorTrain.weightedRecall
            }
        }
    }

    "SVM-MultiIntent-Hyperband-Test-Precision" should "match" in {
        withClue("SVM-MultiIntent-Train-Precision: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentSVMHyperband.testPrecision").toDouble) { multiIntentSVMEvaluatorTest.weightedPrecision
            }
        }
    }

    "SVM-MultiIntent-Hyperband-Test-Recall" should "match" in {
        withClue("SVM-MultiIntent-Test-Recall: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentSVMHyperband.testRecall").toDouble) { multiIntentSVMEvaluatorTest.weightedRecall
            }
        }
    }

}
