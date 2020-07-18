package com.tfs.flashml.systemTests

import com.tfs.flashml.core.{DirectoryCreator, PipelineSteps}
import com.tfs.flashml.util.FlashMLConfig
import com.tfs.flashml.util.conf.FlashMLConstants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

class MultiIntentSVMStdMetricsTest extends AnyFlatSpec{

    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)

    FlashMLConfig.config= ConfigFactory.load("multiIntent_svm_stdMetrics_test_config.json")

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

    //Test cases for Multi Intent SVM CV Std Metrics
    println("=============================================================================================")
    log.info("Starting FlashML test application")
    println("Test case: MultiIntent SVM CV - Std Metrics")

    PipelineSteps.run()

val generatedScoresTest = spark
        .sparkContext
        .textFile(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + new Path(DirectoryCreator.getConfusionMetricsPath(), s"noPageTest").toString + "/part*.txt")
        .collect()

    val generatedScoresTrain = spark
            .sparkContext
            .textFile(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + new Path(DirectoryCreator.getConfusionMetricsPath(), s"noPageTrain").toString + "/part*.txt")
            .collect()

    val expectedTrainResults = Array(
        "1\t\t160\t\t0\t\t1.0\t\t1.0",
        "2\t\t160\t\t0\t\t1.0\t\t1.0",
        "3\t\t160\t\t0\t\t1.0\t\t1.0",
        "4\t\t160\t\t0\t\t1.0\t\t1.0",
        "5\t\t160\t\t0\t\t1.0\t\t1.0"
    )

    "SVM Multi Intent Std Metrics Test " should "match" in {
        withClue("SVM Multi Intent Std Metrics Train"){
            (1 to 5)
                    .foreach(label => {
                assert(generatedScoresTrain(label)==expectedTrainResults(label - 1))
            })
        }
    }

    val expectedTestResults = Array(

        "1\t\t20\t\t4\t\t0.5128205128205128\t\t0.5",
        "2\t\t7\t\t6\t\t0.22580645161290322\t\t0.175",
        "3\t\t10\t\t8\t\t0.23809523809523808\t\t0.25",
        "4\t\t9\t\t7\t\t0.225\t\t0.225",
        "5\t\t15\t\t8\t\t0.3125\t\t0.375"
    )


    "SVM-MultiIntent-StdMetrics Test" should "match" in {
        withClue("SVM MultiIntent Std Metrics Test "){
            (1 to 5)
                    .foreach(label => {
                assert(generatedScoresTest(label)==expectedTestResults(label - 1))

            })
        }
    }

}

