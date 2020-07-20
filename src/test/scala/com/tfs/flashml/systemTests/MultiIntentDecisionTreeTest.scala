package com.tfs.flashml.systemTests

import com.tfs.flashml.core.PipelineSteps
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
import com.tfs.flashml.util.conf.FlashMLConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

class MultiIntentDecisionTreeTest extends AnyFlatSpec{

    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)

    //Load application.conf
    val configSolutionsKeyPair: Config = ConfigFactory.load()

    FlashMLConfig.config= ConfigFactory.load("multiIntent_decisionTrees_test_config.json")

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
            .enableHiveSupport()
            .getOrCreate()

    println("=============================================================================================")
    log.info("Starting FlashML test application")
    println("Test case: MultiIntent Decision Trees")

    PipelineSteps.run()

    import spark.implicits._

    var multiIntentDecisionTreePredictionDF: Array[RDD[(Double,Double)]] = SavePointManager
            .loadData(FlashMLConstants.SCORING)
            .map(_.select("prediction", ConfigValues.getIndexedResponseColumn)
                    .as[(Double, Double)].rdd)

    val multiIntentDecisionTreeEvaluatorTrain = new MulticlassMetrics(multiIntentDecisionTreePredictionDF(0))

    val multiIntentDecisionTreeEvaluatorTest = new MulticlassMetrics(multiIntentDecisionTreePredictionDF(1))

    "DecisionTrees-MultiIntent -YELP - Train-Precision" should "match" in {
        withClue("DecisionTrees-MultiIntent - YELP -Train-Precision: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentDecisionTree.trainPrecision").toDouble) { multiIntentDecisionTreeEvaluatorTrain.weightedPrecision
            }
        }
    }

    "Decision Trees-MultiIntent -YELP - Train-Recall" should "match" in {
        withClue("Decision Trees-MultiIntent-YELP - Train-Recall: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentDecisionTree.trainRecall").toDouble) {
                multiIntentDecisionTreeEvaluatorTrain.weightedRecall
            }

        }
    }


    "Decision Trees-MultiIntent -YELP -Test-Precision" should "match" in {
        withClue("Decision Trees-MultiIntent- YELP -Test-Precision: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentDecisionTree.testPrecision").toDouble) { multiIntentDecisionTreeEvaluatorTest.weightedPrecision
            }

        }
    }

    "Decision Trees-MultiIntent - YELP -Test-Recall" should "match" in {
        withClue("Decision Trees-MultiIntent- YELP  - Test-Recall: ") {
            assertResult(configSolutionsKeyPair.getString("FlashMLTests.multiIntentDecisionTree.testRecall").toDouble) { multiIntentDecisionTreeEvaluatorTest.weightedRecall
            }
        }
    }

}
