package com.tfs.flashml.functionalTests

import com.tfs.flashml.core.metrics.ModelHealthMetricsEvaluator
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{FlashMLConfig}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

import scala.util.Random

class MonitoringMetricsTest extends FlatSpec {

  private val log = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.OFF)

  val configTestScores: Config = ConfigFactory.load()

  FlashMLConfig.config=ConfigFactory.load("psivsi_config.json")

  println("=============================================================================================")
  log.info("Starting FlashML test application")
  println("Test case: Monitoring metrics computation")

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
  import ss.implicits._

  def getBrowser: String = {
    Random.nextInt(5) match {
      case 0 => "safari"
      case 1 => "chrome"
      case 2 => "firefox"
      case 3 => "ie"
      case 4 => "bing"
    }
  }

  def getOS: String = {
    Random.nextInt(5) match {
      case 0 => "windows"
      case 1 => "linux"
      case 2 => "macOS"
      case 3 => "iOS"
      case 4 => "android"
    }
  }

  def getReferrer: String = {
    Random.nextInt(3) match {
      case 0 => "google"
      case 1 => "link"
      case 2 => "ad"
    }
  }

  def getDevice: String = {
    Random.nextInt(4) match {
      case 0 => "tablet"
      case 1 => "phone"
      case 2 => "desktop"
      case 3 => "pc"
    }
  }

  def getGeo: String = {
    Random.nextInt(5) match {
      case 0 => "asia"
      case 1 => "pacific"
      case 2 => "europe"
      case 3 => "us"
      case 4 => "latam"
    }
  }

  val MAXNOP = 10
  Random.setSeed(5)
  val psiBaseDF: DataFrame = Seq.fill(300)(1 + Random.nextInt(MAXNOP), Random.nextFloat()).toDF("pageNumber", "score")
  val psiNewDataDF: DataFrame = Seq.fill(500)(1 + Random.nextInt(MAXNOP), Random.nextFloat()).toDF("pageNumber", "score")

  val vsiBaseDF: DataFrame = Seq.fill(200)(getBrowser, getDevice, getOS, getGeo, getReferrer).toDF("browser", "device", "os", "geo", "referrer")
  val vsiNewDataDF: DataFrame = Seq.fill(500)(getBrowser, getDevice, getOS, getGeo, getReferrer).toDF("browser", "device", "os", "geo", "referrer")

  val modelHealthMonitoring = new ModelHealthMetricsEvaluator()
  val psiTestScores: DataFrame = modelHealthMonitoring.psi(psiBaseDF, "score", MAXNOP, psiNewDataDF, "pageNumber")
  val vsiTestScores: DataFrame = modelHealthMonitoring.vsi(vsiBaseDF, vsiNewDataDF, Array("geo", "browser", "os", "device", "referrer"))


  "psiTestScores" should "match" in {
    (1 to MAXNOP).par.foreach(indexNum => {
      val clueStatement = "PSIsampleTestScore_PageNum" + indexNum.toString
      val resultIndex = "FlashMLTests.psiTestScores.page" + indexNum.toString
      withClue(clueStatement) {
        assertResult(configTestScores.getString(resultIndex).toDouble) {
          psiTestScores.filter(col("pageNumber") === indexNum).select("psiValue").collect()(0)(0)
        }
      }
    })
  }


  val clueStatementBase = "vsiTestScores_"
  "vsiTestScores" should "match" in {
    Array("geo", "os", "browser", "device", "referrer").par.foreach(variable => {
      withClue(clueStatementBase + variable) {
        assertResult(configTestScores.getString("FlashMLTests.vsiTestScores." + variable).toDouble) {
          vsiTestScores.filter($"variable" === variable).select("vsiScore").collect()(0)(0)
        }
      }
    })
  }
}