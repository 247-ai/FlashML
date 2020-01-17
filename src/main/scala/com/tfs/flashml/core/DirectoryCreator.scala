package com.tfs.flashml.core

import com.tfs.flashml.core.metrics.MetricsEvaluator
import com.tfs.flashml.util.{FlashMLConfig, Json}
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Creates folder structure on HDFS for storing training results.
 *
 * @since 1/24/17
 */
object DirectoryCreator
{
    private val log = LoggerFactory.getLogger(getClass)

    val conf = new Configuration()
    conf.set("fs.defaultFS", FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI))
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    conf.set("fs.parameter.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    val fs: FileSystem = FileSystem.get(conf)

    val retrainValue: String = FlashMLConfig.getString(FlashMLConstants.EXPERIMENT_RETRAIN_ID)

    val experimentTypeSuffix: String = FlashMLConfig.getString(FlashMLConstants.EXPERIMENT_TYPE).toLowerCase match
    {
        case model@FlashMLConstants.EXPERIMENT_TYPE_MODEL => "/" + model
        case predict@FlashMLConstants.EXPERIMENT_TYPE_PREDICT => "/" + predict
        case monitoring@FlashMLConstants.EXPERIMENT_TYPE_MONITORING => "/" + monitoring
        case _ => throw new Exception(
            " Experiment Type should be " + FlashMLConstants.EXPERIMENT_TYPE_MODEL + " or " + FlashMLConstants
                    .EXPERIMENT_TYPE_MONITORING + " or " + FlashMLConstants.EXPERIMENT_TYPE_PREDICT + "\n")
    }

    def getBasePath: Path =
    {
        val projectID = FlashMLConfig.getString(FlashMLConstants.FLASHML_PROJECT_ID)
        val modelID = FlashMLConfig.getString(FlashMLConstants.FLASHML_MODEL_ID)
        val basePath = FlashMLConfig.getString(FlashMLConstants.ROOT_DIRECTORY) + "/" + projectID + "/" + modelID +
                "/" + retrainValue + experimentTypeSuffix

        new Path(basePath)
    }

    def createDirectoryStructure(): Unit =
    {
        log.info("Creating/resolving folder structure on hdfs.")
        val directoryArr = ArrayBuffer[Path]()
        log.info(s"\tData path: [${getInputDataPath}]")
        directoryArr.append(getInputDataPath())
        log.info(s"\tMetrics path: [${getMetricsPath}]")
        directoryArr.append(getMetricsPath())
        log.info(s"\tSupport files path: [${getSupportFilesPath}]")
        directoryArr.append(getSupportFilesPath)

        directoryArr.foreach(fs.mkdirs)
    }

    def storeConfusionMatrix(sb: StringBuilder, path: String): Unit =
    {
        val outputFile = fs.create(new Path(path))
        outputFile.write(sb.toString().getBytes)
    }

    def storeMetrics(path: String): Unit =
    {
        val jsonOutputFile = fs.create(new Path(path + ".json"))
        val metricsString = new Json.Value(MetricsEvaluator.metricsMap.toMap).write
        jsonOutputFile.write(metricsString.getBytes)
        val csvOutputFile = fs.create(new Path(path + ".csv"))
        csvOutputFile.write(MetricsEvaluator.csvMetrics.toString().getBytes)
    }

    def deleteDirectory(path: Path): Unit =
    {
        if (fs.exists(path))
        {
            fs.delete(path, true)
        }
    }

    def getInputDataPath(): Path =
    {
        new Path(getBasePath, "input_data")
    }

    def getConfigPath(): Path =
    {
        new Path(getBasePath, "config")
    }

    def getPipelinePath(): Path =
    {
        new Path(getBasePath, "pipelines")
    }


    def getModelPath(): Path =
    {
        new Path(getBasePath
                .toString
                .replace("/" + FlashMLConstants.EXPERIMENT_TYPE_PREDICT, "/" + FlashMLConstants.EXPERIMENT_TYPE_MODEL)
                .replace("/" + FlashMLConstants.EXPERIMENT_TYPE_MONITORING, "/" + FlashMLConstants
                        .EXPERIMENT_TYPE_MODEL))
    }

    def getConfusionMetricsPath(): Path =
    {
        new Path(getMetricsPath, "confusionMetrics")
    }

    def getMetricsPath(): Path =
    {
        new Path(getBasePath, "metrics")
    }

    def getPublishPath(): Path =
    {
        new Path(getBasePath, "publish")
    }

    def getQAPath(): Path =
    {
        new Path(getBasePath, "qa")
    }

    def getSupportFilesPath: Path =
    {
        new Path(getProjectLevelPath, "support_files")
    }

    def getLogisticRegressionModelPath: Path =
    {
        new Path(getModelPath(), FlashMLConstants.LOGISTIC_REGRESSION)
    }

    def getNaiveBayesModelPath: Path =
    {
        new Path(getModelPath(), FlashMLConstants.NAIVE_BAYES)
    }

    def getProjectLevelPath: Path =
    {
        new Path(FlashMLConfig.getString(FlashMLConstants.ROOT_DIRECTORY) + "/" + FlashMLConfig.getString(FlashMLConstants.FLASHML_PROJECT_ID))
    }
}
