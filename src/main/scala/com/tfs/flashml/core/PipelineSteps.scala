package com.tfs.flashml.core

import java.util.concurrent.TimeUnit

import com.tfs.flashml.core.featuregeneration.FeatureGenerationEngine
import com.tfs.flashml.core.metrics.MetricsEvaluator
import com.tfs.flashml.core.modeltraining.ModelTrainingEngine
import com.tfs.flashml.core.preprocessing.PreprocessingEngine
import com.tfs.flashml.core.sampling.TrainTestSampler
import com.tfs.flashml.dal.{DataReaderFactory, SavePointManager}
import com.tfs.flashml.publish.Publish
import com.tfs.flashml.util.ConfigValues.DataSetType
import com.tfs.flashml.util.conf.{ConfigValidator, FlashMLConstants}
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
import com.typesafe.config.ConfigRenderOptions
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer


/**
  * Executes the steps of model training lifecyle.
  *
  * @since 5/12/16.
  */

object PipelineSteps
{
    private val log = LoggerFactory.getLogger(getClass)

    def run(): Unit =
    {
        //Save config parameter on hdfs
        val configFilePath = DirectoryCreator.getBasePath + "/config.json"
        if (ConfigValues.fs.exists(new Path(configFilePath)))
        {
            ConfigValues.fs.delete(new Path(configFilePath), true)
            log.debug("Deleted config parameter on HDFS ")
        }
        val os = ConfigValues.fs.create(new Path(DirectoryCreator.getBasePath + "/config.json"), true)
        os.write(FlashMLConfig.config.root().render(ConfigRenderOptions.concise().setJson(true).setFormatted(true))
                .getBytes)
        os.close()
        log.info(s"Saved config parameter at ${DirectoryCreator.getBasePath}/config.json on HDFS ")
        // Validating config.json
        ConfigValidator.validate()

        val steps: Array[String] = FlashMLConfig
                .getStringArray(FlashMLConstants.PIPELINE_STEPS)
                .map(_.toLowerCase)

        if (steps.contains(FlashMLConstants.QA_DATA_GENERATION) && FlashMLConfig.getString(FlashMLConstants
                .DATE_VARIABLE).isEmpty)
        {
            log.info("[Warning]: Date variable is not mentioned in the config file to generate QA data")
        }

        log.info(s"Steps to Run - " + steps.mkString(sep = ","))

        val startTime = System.nanoTime()
        log.info(s"Start Time: ${TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)} sec")

        // Create Directory Structure
        DirectoryCreator.createDirectoryStructure()

        // Call reader and get data frame
        val inputData: Option[DataFrame] = if (steps.contains(FlashMLConstants.DATAREADER))
            Some(DataReaderFactory.get().read)
        else if (steps.contains(FlashMLConstants.SAMPLING)
                || steps.contains(FlashMLConstants.PREPROCESSING)
                || steps.contains(FlashMLConstants.PUBLISH)
        )
            Some(SavePointManager.loadInputData)
        else None
        log.info(s"Time to acquire data: ${TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)} sec")

        // Train/Test Sampling
        val samplingDF: Option[Array[DataFrame]] = if (steps.contains(FlashMLConstants.SAMPLING)
                || steps.contains(FlashMLConstants.PREPROCESSING)
                || steps.contains(FlashMLConstants.PUBLISH)
        )
            TrainTestSampler.sample(inputData)
        else None

        /**
          * The page level split is happening outside the preprocessing step, because for publishing mleap supported models we need to fit the pipeline again with the data.
          * And if the publish step is running as a separate step then we need to make it available there.
          */
        val pageLevelDataFrameArray: Option[Array[DataFrame]] = if (ConfigValues.isPageLevelModel)
        {
            splitPageLevel(samplingDF)
        }
        else
        {
            None
        }
        // Pre-processing
        // The PreprocessingEngine.process() function also saves the computed DFs to HDFS proj Directory
        val preprocessingDF: Option[Array[DataFrame]] = if (steps.contains(FlashMLConstants.PREPROCESSING))
        {
            if (ConfigValues.isPageLevelModel)
            {
                PreprocessingEngine.process(pageLevelDataFrameArray)
            }
            else PreprocessingEngine.process(samplingDF)

        }
        else if (steps.contains(FlashMLConstants.FEATURE_GENERATION)
                || steps.contains(FlashMLConstants.VECTORIZATION)
        )
        {
            // No IntermediateVariables computed if preprocessing Config empty
            // if NonEmpty we fetch N-D ArrayBuffer of intermediate Variables
            // this N-Dimensional ArrayBuffer defined based on Preprocessing Scope
            // This N-Dimensional ArrayBuffer used to filter columns before caching
            val savedPreprocessedDF = if (PreprocessingEngine
                    .loadPreprocessingConfig
                    .nonEmpty)
            {
                // Loading Preprocessed DFs from HDFS Proj Directory
                // Before loading the DFs, we populate an N-Dimensional ArrayBuffer that stores intermediate generated column names
                // These column names are used to later filter the Preprocessed DFs and then Cache them
                // They are unpersisted after feature Generation step
                PreprocessingEngine.populatePreprocessingIntermediateColumnNames
                SavePointManager.loadData(FlashMLConstants.PREPROCESSING)
            }
            else SavePointManager.loadData(FlashMLConstants.PREPROCESSING)

            Some(savedPreprocessedDF)
        }
        else
            None

        // Cache Preprocessing DF
        if (ConfigValues.pagewisePreprocessingIntVariables.nonEmpty)
            preprocessingDF
                    .map(_.indices
                            .map(index => preprocessingDF
                                    .get
                                    .apply(index)
                                    .drop(ConfigValues.pagewisePreprocessingIntVariables(index / 2): _*)
                                    .cache()))
        else preprocessingDF.map(_.map(_.cache()))

        // Feature Generation
        val featureGenerationDF: Option[Array[DataFrame]] = if (steps.contains(FlashMLConstants.FEATURE_GENERATION)
                || steps.contains(FlashMLConstants.VECTORIZATION))
        {
            FeatureGenerationEngine.process(preprocessingDF)
        }
        else None

        // Unpersist Preprocessing DF
        preprocessingDF.map(_.map(_.unpersist))

        // Vectorization
        val filteredVectorizationDf: Option[Array[DataFrame]] = if (steps.contains(FlashMLConstants.VECTORIZATION))
        {
            // Columns which are needed for next steps.
            val columnsNames = (ConfigValues.primaryKeyColumns ++
                    Array(ConfigValues.topVariable,
                        ConfigValues.pageColumn,
                        FlashMLConstants.FEATURES,
                        ConfigValues.upliftColumn,
                        ConfigValues.responseColumn,
                        ConfigValues.dateVariable,
                        ConfigValues.randomVariable) ++
                    ConfigValues.additionalColumns)
                    .filter(_.nonEmpty)
                    .distinct

            val oVectorizationDF = VectorizationEngine.process(featureGenerationDF)
                    .map(_.map(_.select(columnsNames.map(col): _*)))

            // SavePoint if required
            if (ConfigValues.toSavePoint)
            {
                oVectorizationDF.map(_.map(_.cache))
                oVectorizationDF.foreach(vectorizationDf =>
                {
                    if (ConfigValues.isPageLevelModel)
                    {
                        for (x <- vectorizationDf.indices)
                        {
                            SavePointManager.saveDataFrame(vectorizationDf(x),
                                x % ConfigValues.numPages + 1,
                                DataSetType(x / ConfigValues.numPages),
                                FlashMLConstants.VECTORIZATION)
                        }
                    }
                    else
                    {
                        for (x <- vectorizationDf.indices)
                        {
                            SavePointManager.saveDataFrame(vectorizationDf(x), 0, DataSetType(x), FlashMLConstants
                                    .VECTORIZATION)
                        }
                    }
                })
            }
            oVectorizationDF
        }
        else if (steps.contains(FlashMLConstants.MODELLING) || steps.contains(FlashMLConstants.SCORING))
        {
            val savedVectorizedDf = SavePointManager.loadData(FlashMLConstants.VECTORIZATION)
            Some(savedVectorizedDf)
        }
        else None
        log.info(s"Time till acquiring vectorized data: ${TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)} sec")

        filteredVectorizationDf.map(_.map(_.cache()))
        // Model building or loading
        val modelArray: Option[Array[PipelineModel]] = if (steps.contains(FlashMLConstants.MODELLING))
        {
            ModelTrainingEngine.fit(filteredVectorizationDf)
        }
        else if (steps.contains(FlashMLConstants.SCORING))
        {
            Some(ModelTrainingEngine.loadPipelineArray)
        }
        else None

        log.info(s"Time till training/loading model(s): ${TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)} sec")
        filteredVectorizationDf.map(_.map(_.unpersist()))

        // Model Scoring
        val predictionDf: Option[Array[DataFrame]] = if (steps.contains(FlashMLConstants.SCORING))
        {
            val scoredDf: Option[Array[DataFrame]] = Predict.score(modelArray, filteredVectorizationDf)
            scoredDf
        }

        else if (steps.contains(FlashMLConstants.STANDARD_METRICS) || steps.contains(FlashMLConstants.CUSTOM_METRICS)
                || steps.contains(FlashMLConstants.QA_DATA_GENERATION))
        {
            val savePointedScoredDf = SavePointManager.loadData(FlashMLConstants.SCORING)
            Some(savePointedScoredDf)
        }
        else None
        log.info(s"Time till prediction/loading prediction data: ${TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)} sec")

        // Standard Metrics Evaluation
        if (steps.contains(FlashMLConstants.STANDARD_METRICS))
        {
            predictionDf.foreach(MetricsEvaluator.evaluateStandardMetrics)
            log.info(s"Time till computing standard metrics : ${
                TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() -
                        startTime)
            } sec")
        }

        // Custom Metrics Evaluation
        if (steps.contains(FlashMLConstants.CUSTOM_METRICS) && ConfigValues.isSingleIntent)
        {
            MetricsEvaluator.evaluateCustomMetrics(predictionDf.get, "metrics")
            log.info(s"Time till computing custom metrics: ${TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)} sec")
        }

        // Storing the combined metrics into hdfs in a single JSON file
        val basePath = DirectoryCreator.getBasePath
        val metricsFilePath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + basePath.toString +
                s"/" + s"metrics/metrics.json"

        if (MetricsEvaluator.metricsMap.nonEmpty)
        {
            DirectoryCreator.storeMetrics(metricsFilePath)
            log.info(s"Metrics stored at [$metricsFilePath]")
        }

        // Publish
        if (steps.contains(FlashMLConstants.PUBLISH))
        {
            val format = FlashMLConfig.getString(FlashMLConstants.PUBLISH_FORMAT).toLowerCase()
            format match
            {
                case FlashMLConstants.PUBLISH_FORMAT_JS => Publish.generateJS
                // For mleap publishing, passing the df as parameter because we are fitting it again inside the function.
                case FlashMLConstants.PUBLISH_FORMAT_MLEAP => Publish.generateMleapBundle(if (ConfigValues.isPageLevelModel) pageLevelDataFrameArray else samplingDF)
                case FlashMLConstants.PUBLISH_FORMAT_SPARK => Publish.generateSpark(if (ConfigValues.isPageLevelModel) pageLevelDataFrameArray else samplingDF)
                case _ => new IllegalArgumentException(s"The value provided - '$format' is not supported for publishing")
            }
        }

        if (steps.contains(FlashMLConstants.QA_DATA_GENERATION) && !FlashMLConfig.getString(FlashMLConstants.QA_FORMAT).isEmpty)
        {
            Publish.generateQAData
            log.info(s"Time till generating QA data for TIM: ${TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)} sec")
        }

        log.info(s"Time till completion: ${TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime)} sec")
        log.info("Workflow Complete")
    }

    /**
      * Used for page level models. Each dataset post sampling is further split by page
      * number. The maximum number of page models is given in the config parameter.
      * All the rows for that page and higher are put in one dataframe.
      * For example, if after sampling there are two datasets, Train and Test, and the num of page
      * level models is 6. This will output an array of 12 Dataframes
      *
      * @param odfArray Option - Array of dataframes post sampling split
      * @return Option - Array of dataframes post page level split
      */
    private def splitPageLevel(odfArray: Option[Array[DataFrame]]): Option[Array[DataFrame]] =
    {
        odfArray.map(dfArray =>
        {
            val pageVariableColumnName: String = ConfigValues.pageColumn
            val pageLevelDFArray = new ArrayBuffer[DataFrame]()
            dfArray.foreach
            { df: DataFrame =>
                (1 to ConfigValues.numPages).foreach
                { pageNumber: Int =>
                    val filterCondition =
                        if (pageNumber < ConfigValues.numPages)
                            s"$pageVariableColumnName == $pageNumber"
                        else s"$pageVariableColumnName >= $pageNumber"
                    pageLevelDFArray += df.filter(filterCondition)
                }
            }
            pageLevelDFArray.toArray
        })
    }

}