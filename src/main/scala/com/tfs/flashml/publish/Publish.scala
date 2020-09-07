package com.tfs.flashml.publish

import java.io.File
import java.nio.file.Files

import com.tfs.flashml.core.featuregeneration.FeatureGenerationEngine
import com.tfs.flashml.core.modeltraining.ModelTrainingEngine
import com.tfs.flashml.core.preprocessing.PreprocessingEngine
import com.tfs.flashml.core.{DirectoryCreator, VectorizationEngine}
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.publish.transformer.HotleadTransformer
import com.tfs.flashml.util.conf.FlashMLConstants
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport._
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.tuning.{CrossValidatorCustomModel, HyperBandModel}
import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.sql.functions.{col, concat, udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import resource._

import scala.collection.mutable.ArrayBuffer

/**
  * Main class for publishing models for production deployment.
  *
  * @since 28/11/16.
  */
object Publish
{

    private val log = LoggerFactory.getLogger(getClass)

    /**
      * Method to generate JS files
      */
    def generateJS(): Unit =
    {
        log.info("Saving complete model pipeline in JS format.")
        val ss = SparkSession.builder().getOrCreate()
        val JS = PublishAssembler.generateJS
        val jsPath = new Path(DirectoryCreator.getPublishPath, "js")
        DirectoryCreator.deleteDirectory(jsPath)

        // Write the JS file
        val savePath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + jsPath.toString
        ss.sparkContext.parallelize(Seq(JS), 1).saveAsTextFile(savePath)
        log.info(s"Saved JS files at [$savePath]")
    }

    /**
     * Method to generate the complete pipeline comprising of substeps like preprocessing, vectorization etc.
     * Required for saving as parquet or as mleap bundle.
     * @param trainDF
     */
    private def generateCompletePipeline(trainDF: Option[Array[DataFrame]]) =
    {
        // Make sure we have the dataframes, otherwise we can't proceed
        val pageLevelDFs = trainDF.get
        // For hotlead models (pertaining to web journeys), we have to add a transformer for the hotlead model
        // itself into the pipeline. This transformer essentially contains the business rules (actual
        // thresholds to use for predicting a customer as hotlead). Since this is not related to training,
        // we directly add this transformer in the publish phase.
        val hotleadTransformer = if(ConfigValues.isHotleadModel)
        {
            val thresholds = FlashMLConfig.getDoubleArray(FlashMLConstants.PUBLISH_THRESHOLDS)
            val configTopThresholds = FlashMLConfig.getIntArray(FlashMLConstants.TOP_THRESHOLDS)
            val topVariable = FlashMLConfig.getString(FlashMLConstants.TOP_VARIABLE)
            val customMetricsType = FlashMLConfig.getString(FlashMLConstants.CUSTOM_METRICS_TYPE)
            val nPages = FlashMLConfig.getInt(FlashMLConstants.N_PAGES)
            val topThresholds = 1.to(nPages).map(i =>
            {
                if (configTopThresholds.isEmpty || customMetricsType.equals(FlashMLConstants.PROB_ONLY_CUSTOM_METRICS)) 0
                else
                    configTopThresholds(i - 1).toDouble
            }).toArray
            val inputCols = Array(FlashMLConfig.getString(FlashMLConstants.PAGE_VARIABLE), "probability") ++
                Array(if (!customMetricsType.equals(FlashMLConstants.PROB_ONLY_CUSTOM_METRICS)) topVariable else "").filter(_.nonEmpty).distinct
            // Now set up the transformer
            new HotleadTransformer()
                .setInputCols(inputCols)
                .setOutputCol("isHotLead")
                .setThreshold(thresholds)
                .setTopThresholds(topThresholds)
                .setNPages(nPages)
        }
        else
            null

        // We have to load the pipeline stages of each step to combine it as a single pipeline.
        // We load the pipeline with the checked version, since some of steps may be absent in a pipeline.
        val pageLevelPipelines = new ArrayBuffer[Pipeline]
        val blankStages = Array[Transformer]()
        if (ConfigValues.isPageLevelModel)
        {
            (1 to ConfigValues.numPages).foreach
            {
                pageIndex =>
                    pageLevelPipelines += new Pipeline().setStages(
                        PreprocessingEngine.loadPipelineModelChecked(pageIndex, FlashMLConstants.PREPROCESSING).map(_.stages).getOrElse(blankStages) ++
                        FeatureGenerationEngine.loadPipelineModelChecked(pageIndex, FlashMLConstants.FEATURE_GENERATION).map(_.stages).getOrElse(blankStages) ++
                        VectorizationEngine.loadPipelineModelChecked(pageIndex, FlashMLConstants.VECTORIZATION).map(_.stages).getOrElse(blankStages) ++
                        ModelTrainingEngine.loadPipelineModelChecked(pageIndex, FlashMLConstants.MODEL_TRAINING).map(_.stages).getOrElse(blankStages).drop(1) ++
                        // Add the hotlead model at the end if required
                        (if(ConfigValues.isHotleadModel)
                        {
                            val hotleadPipeline = new Pipeline().setStages(Array(hotleadTransformer))
                            hotleadPipeline.fit(trainDF.get(pageIndex - 1)).stages
                        }
                        else Array[Transformer]())
                    )
            }
        }
        else
        {
            pageLevelPipelines += new Pipeline().setStages(
                PreprocessingEngine.loadPipelineModelChecked(0, FlashMLConstants.PREPROCESSING).map(_.stages).getOrElse(blankStages) ++
                FeatureGenerationEngine.loadPipelineModelChecked(0, FlashMLConstants.FEATURE_GENERATION).map(_.stages).getOrElse(blankStages) ++
                VectorizationEngine.loadPipelineModelChecked(0, FlashMLConstants.VECTORIZATION).map(_.stages).getOrElse(blankStages) ++
                ModelTrainingEngine.loadPipelineModelChecked(0, FlashMLConstants.MODEL_TRAINING).map(_.stages).getOrElse(blankStages).drop(1) ++
                (if (ConfigValues.isHotleadModel)
                {
                    val hotleadPipeline = new Pipeline().setStages(Array(hotleadTransformer))
                    hotleadPipeline.fit(trainDF.get(0)).stages
                }
                else Array[Transformer]())
            )
        }
        // Now edit the pipeline to replace CrossValidatorCustomModel and HyperBandModel with the
        // actual best model from inside those transformers.
        val transformersToReplcae = Array("CrossValidatorCustomModel", "HyperBandModel")
        pageLevelPipelines.foreach(pipeline => {
            val stages = pipeline.getStages
            val cvModelIndex = stages.indexWhere(v => transformersToReplcae.contains(v.getClass.getSimpleName))
            if(cvModelIndex > 0)
            {
                val newStages = stages
                    .zipWithIndex
                    .foldLeft(new ArrayBuffer[Transformer]()){ case(accum, (stage, index)) =>
                        accum += (if(index == cvModelIndex)
                        {
                            if(stage.getClass.getSimpleName == "CrossValidatorCustomModel")
                                stage.asInstanceOf[CrossValidatorCustomModel].bestModel
                            else if(stage.getClass.getSimpleName == "HyperBandModel")
                                stage.asInstanceOf[HyperBandModel].bestModel
                            else
                                stage
                        }
                        else stage).asInstanceOf[Transformer]
                    }
                pipeline.setStages(newStages.toArray)
            }
        })

        // We need to fit the entire pipeline once again to make it serialized in mleap. Because our pipeline
        // stages are in seperate modules. So we need to combine it to one pipeline before serializing.
        val trainedModels = pageLevelPipelines.zip(pageLevelDFs).map{ case(pipeline, df) => pipeline.fit(df) }
        // Return the pipeline model(s)
        trainedModels
    }

    /**
     * Method to generate and write out the complete model pipeline in Spark native format.
     * @param pageLevelTrainDFs
     */
    def generateSpark(pageLevelTrainDFs: Option[Array[DataFrame]]) =
    {
        log.info("Saving complete model pipeline in native (spark) format.")
        /**
         * Save Pipeline model object on disk. Reproduction from trait Engine.
         *
         * @param pageNumber 0 for single intent, Page Number for page level models
         */
        def savePipelineModel(pipelineModel: PipelineModel, pageNumber: Int = 0, step: String): Unit =
        {
            val basePath: Path = DirectoryCreator.getModelPath()
            val pageString: String = if (pageNumber == 0) "noPage" else "page" + pageNumber
            val savePath = s"${FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI)}$basePath/$pageString/noSegment/pipeline/${step}_pipeline"
            pipelineModel
                .write
                .overwrite()
                .save(savePath)
            log.info(s"Saved complete pipeline for [$pageString] at [$savePath]")
        }

        // We need to fit the entire pipeline once again to make it serialized in mleap. Because our pipeline
        // stages are in separate modules. So we need to combine it to one pipeline before serializing.
        val pageLevelTrainedModels = generateCompletePipeline(pageLevelTrainDFs)
        if (ConfigValues.isPageLevelModel)
        {
            pageLevelTrainedModels.zipWithIndex
                .foreach
                {
                    case(pipeLineModel, pageNum) => savePipelineModel(pipeLineModel, pageNum + 1, FlashMLConstants.PUBLISH)
                }
        }
        else
            savePipelineModel(pageLevelTrainedModels(0), 0, FlashMLConstants.PUBLISH)
    }

    /**
     * Method to generate page-level mleap models
     * @param pageLevelTrainDFs
     */
    def generateMleapBundle(pageLevelTrainDFs: Option[Array[DataFrame]]) =
    {
        log.info("Saving complete model pipeline in mleap format.")
        // We need to fit the entire pipeline once again to make it serialized in mleap. Because our pipeline
        // stages are in separate modules. So we need to combine it to one pipeline before serializing.
        val pageLevelTrainedModels = generateCompletePipeline(pageLevelTrainDFs)

        /**
          * Method for saving an MLeap bundle
          * @param df
          * @param pageNum
          */
        def saveMleapBundle(df: DataFrame, pageNum: Int = 0) =
        {
            val pageString: String = if (!ConfigValues.isPageLevelModel) "noPage" else "page" + (pageNum + 1)
            // Get the model for this page
            val mleapModel = pageLevelTrainedModels(pageNum)
            log.info(s"Writing out model in mleap format, consisting of ${mleapModel.stages.length} stages: ${mleapModel.stages.map(s => s.getClass.getSimpleName).mkString(", ")}")
            // We have to transform the pipleine with the dataframe to get the schema of the df containing transformation stages.
            // Easiest way to do that is to take one row from the dataframe and call transform().
            val sbc = SparkBundleContext().withDataset(mleapModel.transform(df.limit(1).toDF()))
            log.debug(s"Dataset schema for spark bundle:\n ${sbc.dataset.get.schema.treeString}")
            val folder = Files.createTempDirectory("FlashMLTemp")
            val file = new File(s"$folder/flashml-$pageString.zip")
            for (bf <- managed(BundleFile(s"jar:file:${file.getPath}")))
            {
                mleapModel.writeBundle.save(bf)(sbc).get
            }
            val savePath = s"${DirectoryCreator.getBasePath}/flashml-$pageString.zip"
            // Delete if there is an old version of the file
            ConfigValues.fs.delete(new Path(savePath), true)
            // Save the new file
            ConfigValues.fs.copyFromLocalFile(new Path(file.getPath), new Path(savePath))
            // Clean up
            if (file.exists()) file.delete()
            if (folder.toFile.exists) folder.toFile.delete()
            log.info(s"Model saved for [$pageString] at [$savePath]")
        }

        if (ConfigValues.isPageLevelModel)
        {
            pageLevelTrainedModels
                .indices
                .foreach(pageNum => saveMleapBundle(pageLevelTrainDFs.get(pageNum), pageNum))
        }
        else
            saveMleapBundle(pageLevelTrainDFs.get(0))
    }

    /**
     * Method to generate QA data for testing.
     */
    def generateQAData(): Unit =
    {
        log.info("Starting QA data generation")
        val spark = SparkSession.builder().getOrCreate()
        val qaPath = DirectoryCreator.getQAPath()
        DirectoryCreator.deleteDirectory(qaPath)
        val primaryKeyVariable = FlashMLConfig.getStringArray(FlashMLConstants.PRIMARY_KEY)
        val inputColumnsArray = ConfigValues.columnVariables.distinct.filterNot(_.isEmpty)
        val pageVariable: String = FlashMLConfig.getString(FlashMLConstants.PAGE_VARIABLE)

        val predictDF = if (ConfigValues.isPageLevelModel)
        {
            // We are loading the page1 df first to get the visitors from the first page. Because only the first page will have all the visitors session available.
            var loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator.getBasePath + "/" +
                    s"/page1/noSegment/data/scoringTrain" + "/*.gz.parquet"

            val inputDF: DataFrame = SavePointManager.loadInputData
            val pageDF: DataFrame = spark.read
                    .load(loadPath)
                    .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))

            if (!FlashMLConfig.hasKey(FlashMLConstants.DATE_VARIABLE) || FlashMLConfig.getString(FlashMLConstants.DATE_VARIABLE).isEmpty)
            {
                val visitorArray = pageDF
                        .select("visitors")
                        .distinct
                        .limit(FlashMLConfig.getInt(FlashMLConstants.QA_DATAPOINTS))
                        .collect
                        .map(r => r.get(0))

                var filteredDF = pageDF
                        .filter(col("visitors").isin(visitorArray: _*))
                        .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                        .select(Array("qaJoinKey", "probability").map(col): _*)

                for (pageNum <- 2 to FlashMLConfig.getInt(FlashMLConstants.EXPERIMENT_NUMBER_OF_PAGES))
                {
                    loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator.getBasePath + "/" +
                            s"/page" + pageNum + "/noSegment/data/scoringTrain" + "/*.gz.parquet"

                    val filteredTempDF = spark
                            .read
                            .load(loadPath)
                            .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))
                            .filter(col("visitors").isin(visitorArray: _*))
                            .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                            .select(Array("qaJoinKey", "probability").map(col): _*)

                    filteredDF = filteredDF.union(filteredTempDF)
                }

                val filteredInputDF = inputDF
                        .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))
                        .filter(col("visitors").isin(visitorArray: _*))
                        .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))

                filteredDF
                        .as("A")
                        .join(filteredInputDF.as("B"), col("A.qaJoinKey") === col("B.qaJoinKey"))
                        .select(inputColumnsArray
                                .map(x => col(s"B.$x")) ++ Array(col("A.probability")): _*)
            }
            else
            {
                //There is a Date Variable
                val dateVariable: String = FlashMLConfig.getString(FlashMLConstants.DATE_VARIABLE)

                val dfHavingMaxCount: String = pageDF
                        .groupBy(dateVariable)
                        .count()
                        .orderBy(desc("count"))
                        .head()
                        .getAs(dateVariable)

                val visitorArray = pageDF
                        .filter(pageDF(dateVariable) === dfHavingMaxCount)
                        .select("visitors")
                        .distinct
                        .limit(FlashMLConfig.getInt(FlashMLConstants.QA_DATAPOINTS))
                        .collect
                        .map(r => r.get(0))

                var filteredDF = pageDF
                        .filter(col("visitors").isin(visitorArray: _*))
                        .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                        .select(Array("qaJoinKey", "probability").map(col): _*)

                for (pageNum <- 2 to FlashMLConfig.getInt(FlashMLConstants.EXPERIMENT_NUMBER_OF_PAGES))
                {
                    loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator.getBasePath + "/" +
                            s"/page" + pageNum + "/noSegment/data/scoringTrain" + "/*.gz.parquet"

                    val filteredTempDF = spark
                        .read
                        .load(loadPath)
                        .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))
                        .filter(col("visitors").isin(visitorArray: _*))
                        .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                        .select(Array("qaJoinKey", "probability").map(col): _*)

                    filteredDF = filteredDF.union(filteredTempDF)
                }

                //Load Input df for getting the input columns
                val filteredInputDF = inputDF
                        .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))
                        .filter(col("visitors").isin(visitorArray: _*))
                        .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))

                filteredDF
                        .as("A")
                        .join(filteredInputDF.as("B"), col("A.qaJoinKey") === col("B.qaJoinKey"))
                        .select(inputColumnsArray
                                .map(x => col(s"B.$x")) ++ Array(col("A.probability")): _*)

            }

        }
        // Non page level model
        else
        {
            val loadPath = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + DirectoryCreator.getBasePath +
                    s"/noPage/noSegment/data/scoringTrain" + "/*.gz.parquet"

            val inputDF = SavePointManager.loadInputData

            val df = spark.read
                    .load(loadPath)
                    .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))

            val visitorArray = if (!FlashMLConfig.hasKey(FlashMLConstants.DATE_VARIABLE) || FlashMLConfig.getString(FlashMLConstants.DATE_VARIABLE).isEmpty)
            {
                // Absence of date variable
                df
                        .select("visitors")
                        .distinct
                        .limit(FlashMLConfig.getInt(FlashMLConstants.QA_DATAPOINTS))
                        .collect
                        .map(r => r.get(0))

            }
            else
            {
                //Presence of date variable
                val dateVariable: String = FlashMLConfig.getString(FlashMLConstants.DATE_VARIABLE)

                val dfHavingMaxCount: String = df
                    .groupBy(dateVariable)
                    .count()
                    .orderBy(desc("count"))
                    .head()
                    .getAs(dateVariable)

                df
                    .filter(df(dateVariable) === dfHavingMaxCount)
                    .select("visitors")
                    .distinct
                    .limit(FlashMLConfig.getInt(FlashMLConstants.QA_DATAPOINTS))
                    .collect
                    .map(r => r.get(0))

            }

            val filteredDF = df
                .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                .filter(col("visitors").isin(visitorArray: _*))
                .select(Array("qaJoinKey", "probability").map(col): _*)

            val filteredInputDF = inputDF
                .withColumn("qaJoinKey", concat((primaryKeyVariable :+ pageVariable).map(col): _*))
                .withColumn("visitors", concat(primaryKeyVariable.map(col): _*))
                .filter(col("visitors").isin(visitorArray: _*))

            filteredDF
                .as("A")
                .join(filteredInputDF.as("B"), col("A.qaJoinKey") === col("B.qaJoinKey"))
                .select(inputColumnsArray.map(x => col(s"B.$x")) ++ Array(col("A.probability")): _*)
        }

        val posProbUDF = udf[Double, DenseVector]((a: DenseVector) => a(1))

        val outputCol = ConfigValues.getColumnNamesVariablesPublishPageLevel(0)

        val testingInfraDF = predictDF
                .withColumn("positive_probability", posProbUDF(col("probability")))
                .select((inputColumnsArray ++ Array("positive_probability"))
                        .distinct
                        .map(col): _*)

        val qaFormat = FlashMLConfig
                .getString(FlashMLConstants.QA_FORMAT)
                .toLowerCase

        if (qaFormat.equals("csv"))
        {
            val csvPath = new Path(qaPath, "csv")
            testingInfraDF
                    .coalesce(1)
                    .write
                    .option("header", "true")
                    .csv(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + csvPath.toString)
            log.info("Saved QA Data in HDFS qa directory of project structure")
        }
        else if (qaFormat.equals("json"))
        {
            val jsPath = new Path(qaPath, "json")
            testingInfraDF
                    .coalesce(1)
                    .write
                    .json(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + jsPath.toString)
        }

        log.info("Saved QA Data in HDFS qa directory of project structure")
    }
}