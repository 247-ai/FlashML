package com.tfs.flashml
import java.io.PrintStream
import com.tfs.flashml.core.PipelineSteps
import com.tfs.flashml.util._
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConverters._

/**
  * Entry point to FlashML application.
  * @since 21/03/18
  */

object FlashML
{
    private val log = LoggerFactory.getLogger(getClass)
    // Redirecting Standard Error Stream to Log4j
    /*val log4jPR = new PrintStream(new LoggingOutputStream(
        Logger.getLogger("com.tfs.flashml.ErrorLogger"), Level.ERROR))

    System.setErr(log4jPR)*/

    def main(args: Array[String]): Unit =
    {
      // Set up the config file to be used
      ConfigValues.configFilePath = if (args.length > 0) args(0) else "config.json"
      log.info(s"Using ${ConfigValues.configFilePath} as config file.")

      log.info("Starting FlashML application")

        val logLevelMsg: Level = FlashMLConfig
                .getString(FlashMLConstants.PACKAGE_LOG_LEVEL)
                .toLowerCase match
        {
            case "debug" => Level.DEBUG
            case "warn" => Level.WARN
            case "off" => Level.OFF
            case "all" => Level.ALL
            case "trace" => Level.TRACE
            case "fatal" => Level.FATAL
            case "info" => Level.INFO
            case _ => Level.ERROR
        }

        // Logging would be controlled for all "org" packages - this will prevent excessive startup logs from (mostly)
        // Apache libraries.
        // To enable logging from individual custom classes within "org", the level needs to be set in the class.
        Logger.getLogger("org").setLevel(logLevelMsg)
        Logger.getLogger("breeze").setLevel(Level.OFF)

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
                .enableHiveSupport()
                .getOrCreate()

        // Get the deploy mode. When running from IntelliJ, this value is not available, so we need a default.
        val deployMode = ss.sparkContext.getConf.get("spark.submit.deployMode", "client")
        log.info(s"Using deploymode: $deployMode, context: $context")
        if(deployMode == "cluster" && context == "local") throw new RuntimeException(s"Invalid combination: deploymode: $deployMode, context: $context. Please update the property flashml.context to 'yarn' in the config file.")

        // Run the FlashML pipeline
        PipelineSteps.run()

        // Stop sparkcontext
        ss.stop()
    }
}
