package com.tfs.flashml.dal

import com.tfs.flashml.core.DirectoryCreator
import com.tfs.flashml.core.sampling.TrainTestSampler
import com.tfs.flashml.util.ConfigUtils._
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.hadoop.fs.Path
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Class to manage "SavePoints", the points in the pipeline where we save the complete dataframe to disk.
 */
object SavePointManager
{
    private val log = LoggerFactory.getLogger(getClass)

    private val inputDataPath: Path = DirectoryCreator.getInputDataPath

    private val modelingMethod: Array[String] = ConfigUtils.modelingMethod

    /**
     * Method to save input dataframe. This is the final dataset (after applying SQL queries mentioned
     * through project.data.queries) that would be used for model building.
     * @param dataFrame
     */
    def saveInputData(dataFrame: DataFrame): Unit =
    {
        val savePath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + inputDataPath
        dataFrame.write.mode(SaveMode.Overwrite).save(savePath)
        log.info(s"Saved input dataframe at [$savePath]")
    }

    /**
     * Method to load input dataframe.
     * @return
     */
    def loadInputData: DataFrame =
    {
        try
            {
                val savePath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + inputDataPath.toString
                log.info(s"Loading input dataframe from savepoint (at [$savePath])")
                SparkSession.builder().getOrCreate().read.load(savePath + "/*.gz.parquet")
            }
        catch
            {
                case e: Throwable =>
                    log.error("Input data path does not exist - add dataReader step to the pipeline")
                    throw e
            }
    }

    /**
     * Save the dataframe on disk. This is currently called post data load, vectorization and scoring step. The dataframe
     * can be for a particular page for web journey data.
     *
     * @param dataFrame - Dataframe to be saved on disk
     * @param pageCount - 0 for single intent, Page Number for pagelevel models
     * @param dataSet   - Train, Test
     * @param process   - Stage eg: Vectorization
     */
    def saveDataFrame(dataFrame: DataFrame, pageCount: Int, dataSet: DataSet, process: String): Unit =
    {
        val pageString: String = if (pageCount == 0) "noPage"
        else "page" + pageCount
        val basePath = DirectoryCreator.getBasePath
        val savePath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + basePath.toString + s"/$pageString/noSegment/data/$process$dataSet"
        dataFrame
          .write
          .mode(SaveMode.Overwrite)
          .save(savePath)
        log.info(s"Saved $process dataframe [$dataSet $pageString] at [$savePath]")
    }

    def saveConfigToHadoop(path:String):Unit = {
        ConfigUtils.fs.copyFromLocalFile(new Path(path),new Path(DirectoryCreator.getBasePath + "/config.json"))
        log.debug("Saved config parameter on HDFS ")
    }

    /**
     * Loading the dataframe from disk
     * @param process Stage eg: Vectorization
     * @return
     */
    def loadData(process: String): Array[DataFrame] =
    {

        val inputData: Option[DataFrame] = Some(loadInputData)
        inputData
          .map(df => {
              // This is used for positive class validation and data balance
              if (ConfigUtils.isSingleIntent) {
                  val labels: Array[_] = TrainTestSampler.findResponseColumnLabels(df)
                  TrainTestSampler.minorityClassLabel = labels(0)
                  TrainTestSampler.majorityClassLabel = labels(1)
              }
          })
        val outputArrayDF: ArrayBuffer[DataFrame] = ArrayBuffer[DataFrame]()
        val dataframeCount = if (FlashMLConfig.getString(FlashMLConstants.SAMPLING_TYPE) == FlashMLConstants.SAMPLING_TYPE_CONDITIONAL) FlashMLConfig.getStringArray(FlashMLConstants.SAMPLE_CONDITION).length
        else FlashMLConfig.getIntArray(FlashMLConstants.SAMPLE_SPLIT).length
        val basePath = DirectoryCreator.getBasePath

        try
            {
                for (i <- 0 until dataframeCount)
                {
                    if (modelingMethod.contains("page_level"))
                    {
                        for (x <- 1 to ConfigUtils.numPages)
                        {
                            val path = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + basePath.toString +
                              s"/page$x/noSegment/data/$process${DataSetType(i)}"
                            outputArrayDF.append(SparkSession.builder().getOrCreate().read.load(path + "/*.gz.parquet"))
                            log.info(s"Loaded $process dataframe ${DataSetType(i)} page$x from savepoint location [$path].")
                        }
                    }
                    else
                    {
                        val path = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + basePath.toString + "/" + s"/noPage/noSegment/data/$process${DataSetType(i)}"
                        outputArrayDF.append(SparkSession.builder().getOrCreate().read.load(path + "/*.gz.parquet"))
                        log.info(s"Loaded $process dataframe ${DataSetType(i)} noPage from savepoint location [$path].")
                    }
                }
            }
        catch
            {
                case e: Throwable =>
                    log.error(s"${process.capitalize} data does not exist - add preceding step to the pipeline")
                    throw e
            }

        outputArrayDF.toArray
    }
}
