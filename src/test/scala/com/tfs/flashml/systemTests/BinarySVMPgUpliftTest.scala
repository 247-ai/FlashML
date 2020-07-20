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

class BinarySVMPgUpliftTest extends AnyFlatSpec
{
    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.OFF)

    //Load application.conf
    val configSolutionsKeyPair: Config = ConfigFactory.load()

    println("=============================================================================================")
    log.info("Starting FlashML test application")
    println("Test Case: SVM Single Intent Page Level Uplift Test Case")

    FlashMLConfig.config = ConfigFactory.load("singleIntent_svm_PGUP_test_config.json")

    val appName = s"${FlashMLConfig.getString(FlashMLConstants.FLASHML_PROJECT_ID)}/${FlashMLConfig.getString(FlashMLConstants.FLASHML_MODEL_ID)}/${FlashMLConfig.getString(FlashMLConstants.FLASHML_JOB_ID)}"
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

    var svmSIPGUPPredictionDF: Array[RDD[(Double, Double)]] = SavePointManager
            .loadData(FlashMLConstants.SCORING)
            .map(_.withColumn("positive_probability", TestUtils.pos_prob(col("modelProbability")))
                    .select("positive_probability", ConfigValues.getIndexedResponseColumn)
                    .as[(Double, Double)].rdd
            )

    val svmSIPGUPEvaluator1 = new BinaryClassificationMetrics(svmSIPGUPPredictionDF(0))
    "SVM-SingleIntent-PGUP-TrainAUC page1" should "match" in
            {
                withClue("SVM-SingleIntent-PGUP-TrainAUC page1: ")
                {
                    assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPGUP.trainAUROCpage1").toDouble)
                    {
                        svmSIPGUPEvaluator1.areaUnderROC()
                    }
                }
            }

    val svmSIPGUPEvaluator2 = new BinaryClassificationMetrics(svmSIPGUPPredictionDF(1))
    "SVM-SingleIntent-PGUP-TrainAUC page2" should "match" in
            {
                withClue("SVM-SingleIntent-PGUP-TrainAUC page2: ")
                {
                    assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPGUP.trainAUROCpage2").toDouble)
                    {
                        svmSIPGUPEvaluator2.areaUnderROC()
                    }
                }
            }

    val svmSIPGUPEvaluator3 = new BinaryClassificationMetrics(svmSIPGUPPredictionDF(2))
    "SVM-SingleIntent-PGUP-TrainAUC page3" should "match" in
            {
                withClue("SVM-SingleIntent-PGUP-TrainAUC page3: ")
                {
                    assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPGUP.trainAUROCpage3").toDouble)
                    {
                        svmSIPGUPEvaluator3.areaUnderROC()
                    }
                }
            }

    val svmSIPGUPEvaluator4 = new BinaryClassificationMetrics(svmSIPGUPPredictionDF(3))
    "SVM-SingleIntent-PGUP-TrainAUC page4" should "match" in
            {
                withClue("SVM-SingleIntent-PGUP-TrainAUC page4: ")
                {
                    assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPGUP.trainAUROCpage4").toDouble)
                    {
                        svmSIPGUPEvaluator4.areaUnderROC()
                    }
                }
            }

    val svmSIPGUPEvaluator5 = new BinaryClassificationMetrics(svmSIPGUPPredictionDF(4))
    "SVM-SingleIntent-PGUP-TestAUC page1" should "match" in
            {
                withClue("SVM-SingleIntent-PGUP-TestAUC page1: ")
                {
                    assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPGUP.testAUROCpage1").toDouble)
                    {
                        svmSIPGUPEvaluator5.areaUnderROC()
                    }
                }
            }

    val svmSIPGUPEvaluator6 = new BinaryClassificationMetrics(svmSIPGUPPredictionDF(5))
    "SVM-SingleIntent-PGUP-TestAUC page2" should "match" in
            {
                withClue("SVM-SingleIntent-PGUP-TestAUC page2: ")
                {
                    assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPGUP.testAUROCpage2").toDouble)
                    {
                        svmSIPGUPEvaluator6.areaUnderROC()
                    }
                }
            }

    val svmSIPGUPEvaluator7 = new BinaryClassificationMetrics(svmSIPGUPPredictionDF(6))
    "SVM-SingleIntent-PGUP-TestAUC page3" should "match" in
            {
                withClue("SVM-SingleIntent-PGUP-TestAUC page3: ")
                {
                    assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPGUP.testAUROCpage3").toDouble)
                    {
                        svmSIPGUPEvaluator7.areaUnderROC()
                    }
                }
            }

    val svmSIPGUPEvaluator8 = new BinaryClassificationMetrics(svmSIPGUPPredictionDF(7))
    "SVM-SingleIntent-PGUP-TestAUC page4" should "match" in
            {
                withClue("SVM-SingleIntent-PGUP-TestAUC page4: ")
                {
                    assertResult(configSolutionsKeyPair.getString("FlashMLTests.svmSingleIntentPGUP.testAUROCpage4").toDouble)
                    {
                        svmSIPGUPEvaluator8.areaUnderROC()
                    }
                }
            }
}
