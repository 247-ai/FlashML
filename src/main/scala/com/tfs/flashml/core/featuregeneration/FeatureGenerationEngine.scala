package com.tfs.flashml.core.featuregeneration

import java.util

import com.tfs.flashml.core.featuregeneration.transformer.{CategoricalColumnsTransformer, GramAssembler, SkipGramGenerator}
import com.tfs.flashml.core.preprocessing.PreprocessingEngine.loadPreprocessingConfig
import com.tfs.flashml.core.{Engine, Validator}
import com.tfs.flashml.util.ConfigValues.{featureGenerationBinningConfig, featureGenerationGramsConfig}
import com.tfs.flashml.util.conf.{ConfigValidatorException, FlashMLConstants}
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
import org.apache.spark.ml.feature.{Bucketizer, NGram, QuantileDiscretizer}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Applies feature generation operations on the sets. Primarily of three types
  * types gram based - skip gram and n gram, categorical column processing and binning of numerical variables
  * Three types of Binning are supported: EquiArea, EquiDistant and Intervals
  * Based on the user specified binning config, the Numerical and Categorical columns are updated.
  */
object FeatureGenerationEngine extends Engine with Validator
{

    override val log: Logger = LoggerFactory.getLogger(getClass)

    //If binning config is empty, numerical columns and categorical columns are not updated
    //For each index of the binning config, the numerical variables to be binned are filtered
    //If binning config is empty, categorical columns are returned
    //Else for each binning config pagewise, we extract the binned column names and return it to categorical columns

    def process(odfArray: Option[Array[DataFrame]]): Option[Array[DataFrame]] =
    {

        odfArray.map(dfArray =>
        {
            if (ConfigValues.isModel)
            {

                if (FlashMLConfig
                        .config
                        .getList(FlashMLConstants.EXPERIMENT_FEATURE_GENERATION_GRAMS)
                        .unwrapped()
                        .asInstanceOf[java.util.ArrayList[Any]].isEmpty && ConfigValues.categoricalColumns.asInstanceOf[Array[Any]].isEmpty)
                    return Some(dfArray)
                else
                {
                    if (!FlashMLConfig
                            .config.getList(FlashMLConstants.EXPERIMENT_FEATURE_GENERATION_GRAMS)
                            .unwrapped()
                            .asInstanceOf[java.util.ArrayList[Any]].isEmpty)
                    {
                        log.info(s"Feature Generation: Loading config.")
                    }

                    if (ConfigValues.isPageLevelModel)
                    {
                        log.info(s"Feature Generation: Page Level Processing.")

                        (1 to ConfigValues.numPages)
                                .foreach
                                { pageNumber: Int =>
                                    log.info(s"Feature Generation: Building Pipeline for Page $pageNumber.")
                                    pipelineModelArray += buildPipelineModel(dfArray(pageNumber - 1), pageNumber)
                                }

                        dfArray
                                .indices
                                .foreach
                                { index: Int =>
                                    outputDFArrayBuffer += pipelineModelArray(index % ConfigValues.numPages).
                                            transform(dfArray(index))
                                }

                    }
                    else
                    {
                        pipelineModelArray += buildPipelineModel(dfArray(0), 0)
                        dfArray.foreach
                        { df: DataFrame =>
                            outputDFArrayBuffer += pipelineModelArray(0).transform(df)
                        }
                    }
                }
            }
            else
            {
                if (ConfigValues.isPageLevelModel)
                {
                    (1 to ConfigValues.numPages).foreach
                    { pageNumber: Int =>
                        pipelineModelArray += loadPipelineModel(pageNumber)
                    }

                    dfArray.indices.foreach
                    { index: Int =>
                        outputDFArrayBuffer += pipelineModelArray(index % ConfigValues.numPages)
                                .transform(dfArray(index))
                    }
                }
                else
                {
                    pipelineModelArray += loadPipelineModel(0)

                    dfArray
                            .foreach
                            { df: DataFrame =>
                                outputDFArrayBuffer += pipelineModelArray(0)
                                        .transform(df)
                            }
                }
            }
            outputDFArrayBuffer.toArray
        })
    }

    override def buildPipelineModel(df: DataFrame, pageCount: Int): PipelineModel =
    {

        //Holder for pipeline stages
        val allStages = ArrayBuffer[PipelineStage]()

        val pageNum = if (pageCount == 0) 0
        else pageCount - 1

        if (featureGenerationBinningConfig.nonEmpty)
        {

            ConfigValues.featureGenerationScope match
            {

                case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>

                    log.info(s"Feature Generation: Adding Binning transformer for page number " + pageNum + " to the pipeline")
                    allStages ++= getBinningPipelineStages(featureGenerationBinningConfig.asInstanceOf[Array[util.HashMap[String, Any]]], df, pageNum)

                case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>
                    if (featureGenerationBinningConfig.isDefinedAt(pageNum) && featureGenerationBinningConfig(pageNum).asInstanceOf[Array[util.HashMap[String, Any]]].nonEmpty)
                    {
                        val pageSpecificBinningConfig = featureGenerationBinningConfig(pageNum).asInstanceOf[Array[util.HashMap[String, Any]]]

                        log.info(s"Feature Generation: Adding Binning transformer for page number " + pageNum + " to the pipeline")
                        allStages ++= getBinningPipelineStages(pageSpecificBinningConfig, df, pageNum)

                    }

            }

        }

        //Check if categorical columns are present and process them
        ConfigValues.featureGenerationScope match
        {
            case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>

                if (ConfigValues.categoricalColumns1DArray.nonEmpty)
                {
                    log.info(s"Feature Generation: Adding categorical concat transformer to the pipeline")

                    val inputColArray = ConfigValues.categoricalColumns1DArray

                    val outputCol = FlashMLConstants.CATEGORICAL_ARRAY

                    val stage = new CategoricalColumnsTransformer()
                            .setInputCols(inputColArray)
                            .setOutputCol(outputCol)

                    allStages += stage
                }

                if (featureGenerationGramsConfig
                        .asInstanceOf[Array[util.HashMap[String, Any]]].nonEmpty)
                {

                    val featureGenerationMapArray = featureGenerationGramsConfig.asInstanceOf[Array[util.HashMap[String, Any]]]

                    for (featureGenerationMap <- featureGenerationMapArray)
                    {
                        allStages ++= getGramsPipelineStages(featureGenerationMap)
                    }
                }


            case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>

                if (ConfigValues.categoricalColumns2DArray.nonEmpty && ConfigValues.categoricalColumns2DArray
                        .isDefinedAt(pageNum) && ConfigValues.categoricalColumns2DArray(pageNum).nonEmpty)
                {

                    log.info(s"Feature Generation: Adding categorical concat transformer to the pipeline")

                    val inputColArray = ConfigValues.categoricalColumns2DArray(pageNum)

                    val outputCol = FlashMLConstants.CATEGORICAL_ARRAY

                    val stage = new CategoricalColumnsTransformer()
                            .setInputCols(inputColArray)
                            .setOutputCol(outputCol)

                    allStages += stage
                }

                if (featureGenerationGramsConfig.nonEmpty && featureGenerationGramsConfig
                        .isDefinedAt(pageNum))
                {

                    val featureGenerationMapArray = featureGenerationGramsConfig(pageNum)
                            .asInstanceOf[Array[util.HashMap[String, Any]]]

                    //featureGenerationMap is the config for one column
                    for (featureGenerationMap <- featureGenerationMapArray)
                    {
                        allStages ++= getGramsPipelineStages(featureGenerationMap)
                    }

                }

            case _ =>
                log.info(ConfigValues.scopeProcessingErrorMessage)
                throw new Exception(ConfigValues.scopeProcessingErrorMessage)

        }

        //Build Pipeline
        val featureGenerationPipeline: Pipeline = new Pipeline()
                .setStages(allStages.toArray)

        //Fit the pipeline of the dataframe and then save
        val featureGenerationModel = featureGenerationPipeline.fit(df)
        savePipelineModel(featureGenerationModel, pageCount)

        featureGenerationModel
    }

    /**
      * @param pageSpecificBinningConfig the Binning Config for the corresponding page
      * @param df                        the preprocessed DataFrame on which the binning transformers are applied
      * @param pageNum                   the PageNumber on which Binning is operated. Reqd. for labelling the output binned column
      * @return Array of Pipeline stages for each of the input binning variables for the given page
      */
    private def getBinningPipelineStages(pageSpecificBinningConfig: Array[java.util.HashMap[String, Any]], df: DataFrame, pageNum: Int): Array[PipelineStage] =
    {

        //The binningConfigPageVariables value may be a 2D or 1D Array
        val binnedColumnsPageSpecific = ConfigValues.featureGenerationScope match
        {

            case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>
                ConfigValues
                        .binningConfigPageVariables
                        .asInstanceOf[Array[(String, String)]]
                        .toMap

            case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
                ConfigValues
                        .binningConfigPageVariables(pageNum)
                        .asInstanceOf[Array[(String, String)]]
                        .toMap
        }

        pageSpecificBinningConfig
                .map(transformationObj =>
                {
                    //For each binning variable, we extract the input column, type of binning and type associated parameter and then pass the binning transformer with the user specified parameters
                    val binningType = transformationObj(FlashMLConstants.BINNING_TYPE).asInstanceOf[String]
                    val inputCol = transformationObj(FlashMLConstants.INPUT_VARIABLE).asInstanceOf[String]
                    val outputCol: String = binnedColumnsPageSpecific(inputCol)

                    log.info(s"Feature Generation: Binning for variable " + inputCol)

                    binningType.toLowerCase match
                    {

                        case FlashMLConstants.BINNING_TYPE_EQUIDISTANT =>
                            val numberOfBuckets = transformationObj(FlashMLConstants.INPUT_PARAMETER).asInstanceOf[Int]

                            //fetch max and min values in current df for input col
                            // create list of intervals and apply bucketizer
                            val maxValueMinValueRow: Row = df
                                    .select(inputCol)
                                    .agg(max(inputCol), min(inputCol))
                                    .head()

                            val minValue: Double =
                                maxValueMinValueRow
                                        .getInt(1)
                                        .asInstanceOf[Double]

                            val maxValue: Double = maxValueMinValueRow
                                    .getInt(0)
                                    .asInstanceOf[Double]

                            val interval: Double = (maxValue - minValue) / numberOfBuckets

                            val bucketIntervals: Array[Double] = (minValue to maxValue by interval).toArray

                            val splits: Array[Double] = (ArrayBuffer(Double.NegativeInfinity) ++ bucketIntervals ++ Array(Double.PositiveInfinity))
                                    .toArray

                            new Bucketizer()
                                    .setInputCol(inputCol)
                                    .setOutputCol(outputCol)
                                    .setSplits(splits)

                        case FlashMLConstants.BINNING_TYPE_EQUIAREA =>
                            //Iterate through df sorted by values for column 'inputCol'
                            //Discrete quantizer works on percentile approach
                            val numberOfBuckets = transformationObj(FlashMLConstants.INPUT_PARAMETER).asInstanceOf[Int]

                            new QuantileDiscretizer()
                                    .setInputCol(inputCol)
                                    .setOutputCol(outputCol)
                                    .setNumBuckets(numberOfBuckets)

                        case FlashMLConstants.BINNING_TYPE_INTERVALS =>
                            //Splits are already mentioned by user
                            //return bucketizer on the splits
                            val splits = transformationObj(FlashMLConstants.INPUT_PARAMETER)
                                    .asInstanceOf[java.util.ArrayList[Int]]
                                    .asScala
                                    .map(_.toDouble)
                                    .toArray

                            new Bucketizer()
                                    .setInputCol(inputCol)
                                    .setOutputCol(outputCol)
                                    .setSplits((ArrayBuffer(Double.NegativeInfinity) ++ splits.to[ArrayBuffer] ++ ArrayBuffer(Double.PositiveInfinity)).toArray)
                    }
                })

    }

    /**
      * Create an array with the operations on the column. Gram operations are applied on the input column specified.
      * Finally, the newly created gram columns along with the original sequence are assembled
      */
    private def getGramsPipelineStages(map: util.HashMap[String, Any]): ArrayBuffer[PipelineStage] =
    {
        val inputColumnName = map
                .get(FlashMLConstants.INPUT_VARIABLE)
                .toString
        val outputColumnName = map
                .get(FlashMLConstants.OUTPUT_VARIABLE)
                .toString
        val operations = map.get("transformations")
                .asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
                .asScala

        log.info(s"Feature Generation: Processing variable '$inputColumnName'.")

        val pipelineStageArray = ArrayBuffer[PipelineStage]()

        //To store the output column names of the gram transformers.
        //This is finally passed as input variable to gram addition transformer
        val intermediateColumnArray: ArrayBuffer[String] = ArrayBuffer(inputColumnName)

        for (op <- operations)
        {
            val key = op
                    .keySet
                    .asScala
                    .head
            pipelineStageArray ++= getStage(key, op(key))
        }

        log.info(s"Feature Generation: Adding Gram Assembler transformer. Columns added: [${intermediateColumnArray.mkString(",")}]")
        pipelineStageArray += new GramAssembler()
                .setInputCols(intermediateColumnArray.toArray)
                .setOutputCol(outputColumnName)

        /**
          * Returns the feature generation transformer which is a PipelineStage
          *
          * @param opName   Name of the operation. Eg. Tokenizer, Regex Replacement
          * @param opParams Parameters needed to create the operation transformer. Eg.
          *                 Regex pattern for Tokenizer
          * @return
          */
        def getStage(opName: String, opParams: Any): Array[PipelineStage] =
        {

            log.info(s"Feature Generation: Adding '$opName' to the pipeline.")

            opName.toLowerCase match
            {

                case FlashMLConstants.N_GRAM =>

                    val ngramStageArray = ArrayBuffer[PipelineStage]()

                    opParams
                            .asInstanceOf[java.util.ArrayList[Int]]
                            .asScala
                            .foreach
                            { n: Int =>
                                val outputCol = inputColumnName + "_" + opName + "_" + n
                                intermediateColumnArray += outputCol
                                ngramStageArray += new NGram()
                                        .setN(n)
                                        .setInputCol(inputColumnName)
                                        .setOutputCol(outputCol)
                            }

                    ngramStageArray.toArray

                case FlashMLConstants.SKIP_GRAM =>

                    val outputCol = inputColumnName + "_" + opName
                    intermediateColumnArray += outputCol

                    val andRulesGen = new SkipGramGenerator()
                            .setInputCol(inputColumnName)
                            .setOutputCol(outputCol)
                            .setWindowSize(opParams.asInstanceOf[Int])

                    Array(andRulesGen)

                case _ =>
                    log.error(s"Feature generation step not found")
                    throw new Exception("Feature generation step not found")
            }
        }

        pipelineStageArray
    }

    val savePipelineModel: (PipelineModel, Int) => Unit = savePipelineModel(_: PipelineModel, _: Int, FlashMLConstants.FEATURE_GENERATION)

    val loadPipelineModel: Int => PipelineModel = loadPipelineModel(_: Int, FlashMLConstants.FEATURE_GENERATION)

    /**
      * Validating flashml featuregeneration configuration for checking whether the input variables are part of preprocessing output variables
      * and checking whether the transformations in the config are supported
      */
    override def validate(): Unit =
    {
        // supported transformations
        val transformations = List[String](
            FlashMLConstants.N_GRAM,
            FlashMLConstants.SKIP_GRAM
        )
        // validate only if feature generation grams config is non empty
        if (featureGenerationGramsConfig.nonEmpty)
        {
            // Validating based on page level and scope
            if (ConfigValues.isPageLevelModel)
            {
                (0 to ConfigValues.numPages - 1).foreach(pageNumber =>
                {
                    //loading preprocessing  steps based on a preprocessing scope
                    val preprocessingStepsPageWise = ConfigValues.preprocessingVariablesScope match
                    {
                        case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => loadPreprocessingConfig
                        case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => loadPreprocessingConfig(pageNumber)
                    }
                    //fetching text variables
                    val textVariables = ConfigValues.variablesScope match
                    {
                        case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => ConfigValues.scopeTextVariables.asInstanceOf[Array[String]]
                        case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => ConfigValues.scopeTextVariables(pageNumber).asInstanceOf[Array[String]]
                    }
                    // validating based on featuregeneration scope
                    ConfigValues.featureGenerationScope match
                    {
                        case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
                            if (featureGenerationGramsConfig
                                    .asInstanceOf[Array[util.HashMap[String, Any]]].nonEmpty)
                            {
                                // feature generation grams array from config
                                val featureGenerationGrams = featureGenerationGramsConfig.asInstanceOf[Array[util.HashMap[String, Any]]]
                                // fetching valid preprocessing output variables
                                val preprocessingOutputVars = preprocessingStepsPageWise
                                        .asInstanceOf[Array[util.HashMap[String, Any]]]
                                        .filter(obj => textVariables.contains(obj.get(FlashMLConstants.INPUT_VARIABLE).toString))
                                        .map(obj =>
                                        {
                                            obj
                                                    .get(FlashMLConstants.OUTPUT_VARIABLE)
                                                    .toString
                                        })
                                // function call for validation
                                variableDependencyValidation(featureGenerationGrams, preprocessingOutputVars, transformations)
                            }


                        case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>
                            if (featureGenerationGramsConfig.nonEmpty && featureGenerationGramsConfig
                                    .isDefinedAt(pageNumber))
                            {
                                // feature generation grams array from config for each page
                                val featureGenerationGrams = featureGenerationGramsConfig(pageNumber).asInstanceOf[Array[util.HashMap[String, Any]]]
                                // fetching valid preprocessing output variables for each page
                                val preprocessingOutputVars = preprocessingStepsPageWise
                                        .asInstanceOf[Array[util.HashMap[String, Any]]]
                                        .filter(obj => textVariables.contains(obj.get(FlashMLConstants.INPUT_VARIABLE).toString))
                                        .map(obj =>
                                        {
                                            obj
                                                    .get(FlashMLConstants.OUTPUT_VARIABLE)
                                                    .toString
                                        })
                                // function call for validation
                                variableDependencyValidation(featureGenerationGrams, preprocessingOutputVars, transformations)
                            }

                        case _ =>
                            log.error(s" Wrong feature generation scope entered")
                            throw new Exception(s" Wrong feature generation scope entered")

                    }
                })
            }
            else
            {
                //throwing an error if the scope is perpage or allpage for non page level variables
                ConfigValues.featureGenerationScope match
                {
                    case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
                        val msg = s"Scope cannot be ${ConfigValues.featureGenerationScope} for non page level model"
                        log.error(msg)
                        throw new ConfigValidatorException(msg)
                    case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>
                        if (featureGenerationGramsConfig
                                .asInstanceOf[Array[util.HashMap[String, Any]]].nonEmpty)
                        {
                            // feature generation grams array from config for each page
                            val featureGenerationGrams = featureGenerationGramsConfig.asInstanceOf[Array[util.HashMap[String, Any]]]
                            //fetching text variables
                            val textVariables = ConfigValues.scopeTextVariables.asInstanceOf[Array[String]]
                            // fetching valid preprocessing output variables for each page
                            val preprocessingOutputVars = loadPreprocessingConfig
                                    .asInstanceOf[Array[util.HashMap[String, Any]]]
                                    .filter(obj => textVariables.contains(obj.get(FlashMLConstants.INPUT_VARIABLE).toString))
                                    .map(obj =>
                                    {
                                        obj
                                                .get(FlashMLConstants.OUTPUT_VARIABLE)
                                                .toString
                                    })
                            // function call for validation
                            variableDependencyValidation(featureGenerationGrams, preprocessingOutputVars, transformations)
                        }
                }
            }
        }
    }

    /**
      * Validating whether the featuregeneration step variable is same as preprocessing variable
      *
      * @param featureGenerationSteps  featuregeneration Steps
      * @param preprocessingOutputVars List of valid text variables on which feature generation can be applied
      * @param transformations         List of valid transformations
      */
    def variableDependencyValidation(featureGenerationSteps: Array[util.HashMap[String, Any]], preprocessingOutputVars: Array[String], transformations: List[String]): Unit =
    {
        featureGenerationSteps
        .foreach(step =>
        {
            val inputvar = step
                    .get(FlashMLConstants.INPUT_VARIABLE)
                    .toString
            if (!preprocessingOutputVars.contains(inputvar))
            {
                val msg = s"$inputvar Input Variable is not part of the Preprocessing output variables"
                log.error(msg)
                throw new ConfigValidatorException(msg)
            }

            step.get(FlashMLConstants.PREPROCESSING_TRANSFORMATIONS)
                .asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
                .asScala
                .map(_.asScala)
                .toArray.foreach(transform =>
                {
                    if (!transformations.contains(transform.keySet.head))
                    {
                        val msg = s"${transform.keySet.head} Transformation not supported"
                        log.error(msg)
                        throw new ConfigValidatorException(msg)
                    }
                })
        })
    }
}
