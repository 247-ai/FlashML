package com.tfs.flashml.systemTests

import com.tfs.flashml.TestUtils
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
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

class BinarySVMUpliftTest extends AnyFlatSpec
{

    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)

    //Load application.conf
    val configSolutionsKeyPair: Config = ConfigFactory.load()

    println("=============================================================================================")
    log.info("Starting FlashML test application")
    println("Test Case: SVM Single Intent Non Page Uplift Test Case")

    FlashMLConfig.config = ConfigFactory.load("singleIntent_svm_UP_test_config.json")

    val appName = s"${FlashMLConfig.getString(FlashMLConstants.FLASHML_PROJECT_ID)}/${FlashMLConfig.getString(FlashMLConstants.FLASHML_MODEL_ID)}/${FlashMLConfig.getString(FlashMLConstants
            .FLASHML_JOB_ID)}"
    val context = FlashMLConfig.getString(FlashMLConstants.CONTEXT)
    val HIVE_METASTORE_KEY = "hive.metastore.uris"
    val HIVE_METASTORE_THRIFT_URL = FlashMLConfig.getString(FlashMLConstants.HIVE_THRIFT_URL)

    val ss = SparkSession.builder()
            .config(HIVE_METASTORE_KEY, HIVE_METASTORE_THRIFT_URL)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrator", "com.tfs.flashml.util.FlashMLKryoRegistrator")
            .config("spark.extraListeners", "com.tfs.flashml.util.CustomSparkListener")
            .config("spark.kryoserializer.buffer.max", "256")
            .config("spark.sql.parquet.compression.codec", "gzip")
            .config("spark.ui.showConsoleProgress", "False")
            .master(context)
            .appName(appName)
            .enableHiveSupport().getOrCreate()

    PipelineSteps.run()

    import ss.implicits._

    var svmSIUPPredictionDF: Array[RDD[(Double, Double)]] = SavePointManager.loadData(FlashMLConstants.SCORING)
            .map(_.withColumn("positive_probability", TestUtils.pos_prob(col("modelProbability")))
                    .select("positive_probability", ConfigValues.getIndexedResponseColumn).as[(Double, Double)].rdd)

    val svmSIUPEvaluator1 = new BinaryClassificationMetrics(svmSIUPPredictionDF(0))
    "SVM-SingleIntent-UP-TrainAUC" should "match" in
            {
                withClue("SVM-SingleIntent-UP-TrainAUC: ")
                {
                    assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentUP.trainAUROC").toDouble)
                    {
                        svmSIUPEvaluator1.areaUnderROC()
                    }
                }
            }

    val svmSIUPEvaluator2 = new BinaryClassificationMetrics(svmSIUPPredictionDF(1))
    "SVM-SingleIntent-UP-TestAUC" should "match" in
            {
                withClue("SVM-SingleIntent-UP-TestAUC: ")
                {
                    assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentUP.testAUROC").toDouble)
                    {
                        svmSIUPEvaluator2.areaUnderROC()
                    }
                }
            }

}
