package com.tfs.flashml.core

import java.util
import com.tfs.flashml.core.preprocessing.PreprocessingEngine.{loadPreprocessingConfig, log}
import com.tfs.flashml.util.ConfigUtils.{binningConfigPageVariables, featureGenerationGramsConfig}
import com.tfs.flashml.util.conf.{ConfigValidatorException, FlashMLConstants}
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
 * Applies vectorization on columns of the dataframe followed by creating feature column
 * using Vector Assembler
 *
 */
object VectorizationEngine extends Engine with Validator
{

  override val log: Logger = LoggerFactory.getLogger(getClass)

  def process(odfArray: Option[Array[DataFrame]]): Option[Array[DataFrame]] =
  {

    odfArray.map(dfArray =>
    {
      if (ConfigUtils.isModel)
      {
        if (ConfigUtils.isPageLevelModel)
        {
          log.info(s"Vectorization: Page level processing.")
          (1 to ConfigUtils.numPages)
            .foreach
            { pageNumber: Int =>
              log.info(s"Vectorization: Building Pipeline for Page $pageNumber.")
              pipelineModelArray += buildPipelineModel(dfArray(pageNumber - 1), pageNumber)
            }

          dfArray.indices.foreach
          { index: Int =>
            outputDFArrayBuffer += pipelineModelArray(index % ConfigUtils.numPages).transform(dfArray
            (index))
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
      else
      {
        if (ConfigUtils.isPageLevelModel)
        {
          (1 to ConfigUtils.numPages).foreach
          { pageNumber: Int =>
            pipelineModelArray += loadPipelineModel(pageNumber)
          }

          dfArray.indices.foreach
          { index: Int =>
            outputDFArrayBuffer += pipelineModelArray(index % ConfigUtils.numPages).transform(dfArray
            (index))
          }
        }
        else
        {
          pipelineModelArray += loadPipelineModel(0)

          dfArray.foreach
          { df: DataFrame =>
            outputDFArrayBuffer += pipelineModelArray(0).transform(df)
          }
        }
      }

      outputDFArrayBuffer.toArray
    })
  }

  override def buildPipelineModel(df: DataFrame, pageCount: Int): PipelineModel =
  {

    // Container for pipeline stages
    val allStages = ArrayBuffer[PipelineStage]()

    // Text Column Vectorization
    val textVectorizationConfig = ConfigUtils.loadTextVectorizationConfig

    if (textVectorizationConfig.nonEmpty)
    {

      val textVectorizationMapArr = ConfigUtils.textVectorizationScope.toLowerCase match
      {

        case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>

          ConfigUtils
            .loadTextVectorizationConfig
            .asInstanceOf[Array[util.HashMap[String, Any]]]

        case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>

          val currTextVectorConfig = ConfigUtils
            .loadTextVectorizationConfig
            .asInstanceOf[Array[Array[util.HashMap[String, Any]]]]

          val pageNum = if (pageCount == 0) 0
          else pageCount - 1

          if (currTextVectorConfig.isDefinedAt(pageNum) && currTextVectorConfig(pageNum).nonEmpty)
            currTextVectorConfig(pageNum)
          else Array[util.HashMap[String, Any]]()
      }

      //featureGenerationMap is the config for one column
      for (textVectorizationMap <- textVectorizationMapArr)
      {
        allStages ++= getPipelineStagesForColumn(textVectorizationMap)
      }
    }

    /**
     * Categorical Vectorization - This takes the output of categorical concat transformer and
     * applies vectorization on it. We first check whether the required parameters are present in the config
     * file. Next we check if the categorical array column is present in the dataset. Then we proceed.
     */
    if (FlashMLConfig.getString(FlashMLConstants.SLOTS_CATEGORICAL_VARIABLES).nonEmpty
      && FlashMLConfig.getString(FlashMLConstants.CATEGORICAL_VARIABLES_VECTORIZATION_METHOD).nonEmpty
      && df.columns.contains(FlashMLConstants.CATEGORICAL_ARRAY))
      allStages ++= getCategoricalVectorizationStage

    log.info(s"Vectorization: Adding Vector Assembler to the pipeline")

    // Column to be assembled into a feature vector. For each page, in case of page level model
    val columnsToAssemble: Array[String] =
      ConfigUtils.getColumnsToAssemble(if (pageCount == 0) 0
      else pageCount - 1)

    val vectorAssembler = new VectorAssembler()
      .setInputCols(columnsToAssemble)
      .setOutputCol(FlashMLConstants.FEATURES)

    allStages += vectorAssembler

    // Build Pipeline
    val vectorizationPipeline = new Pipeline()
      .setStages(allStages.toArray)

    // Fit the pipeline of the dataframe and then save
    val vectorizationModel = vectorizationPipeline.fit(df)
    savePipelineModel(vectorizationModel, pageCount)

    vectorizationModel
  }

  private def getPipelineStagesForColumn(map: util.HashMap[String, Any]): Array[PipelineStage] =
  {

    val inputColumnName = map
      .get(FlashMLConstants.INPUT_VARIABLE)
      .asInstanceOf[String]

    val opName = map
      .get(FlashMLConstants.VECTORIZATION_TEXT_METHOD)
      .asInstanceOf[String]

    val vectorSize = map
      .get(FlashMLConstants.VECTORIZATION_TEXT_SLOT_SIZE)
      .asInstanceOf[Int]

    log.info(s"Vectorization: Processing variable '$inputColumnName'.")

    getStage(opName, vectorSize, inputColumnName)

  }


  /**
   * Method to return the PipelineStage objects corresponding to an operation.
   */
  private def getStage(opName: String, vectorSize: Int, inputColumnName: String): Array[PipelineStage] =
  {

    val outputColumn = inputColumnName + "_" + opName

    log.info(s"Vectorization: Adding '$opName' to the pipeline.")

    opName match
    {

      case FlashMLConstants.HASHING_TF => Array(new HashingTF()
        .setInputCol(inputColumnName)
        .setOutputCol(outputColumn)
        .setNumFeatures(vectorSize))

      case FlashMLConstants.COUNT_VECTORIZER => Array(new CountVectorizer()
        .setInputCol(inputColumnName)
        .setOutputCol(outputColumn)
        .setVocabSize(vectorSize))

      case FlashMLConstants.WORD2VEC => Array(new Word2Vec()
        .setInputCol(inputColumnName)
        .setOutputCol(outputColumn)
        .setVectorSize(vectorSize))

      case FlashMLConstants.TF_IDF =>
        val tf = new CountVectorizer()
          .setInputCol(inputColumnName)
          .setOutputCol(outputColumn + "_tf_col")
          .setVocabSize(vectorSize)
        val idf = new IDF()
          .setInputCol(tf.getOutputCol)
          .setOutputCol(outputColumn)
        Array(tf, idf)

      case _ =>
        log.error(s"Vectorization step $opName not found")
        throw new UnsupportedOperationException(s"Not supported: $opName")
    }
  }

  def getCategoricalVectorizationStage: Array[PipelineStage] =
  {

    log.info(s"Vectorization: Processing Categorical Array")

    val opName = FlashMLConfig.getString(FlashMLConstants.CATEGORICAL_VARIABLES_VECTORIZATION_METHOD)
    val vectorSize = FlashMLConfig.getInt(FlashMLConstants.SLOTS_CATEGORICAL_VARIABLES)
    val inputColumn = FlashMLConstants.CATEGORICAL_ARRAY

    getStage(opName, vectorSize, inputColumn)
  }

  def getVariablesForTextVectorization(map: util.HashMap[String, String]): (String, Array[String]) =
  {

    val inputVariable = map.get(FlashMLConstants.INPUT_VARIABLE)
    val keys: scala.collection.mutable.Set[String] = JavaConversions.asScalaSet(map.keySet())
    val toRemove = List(FlashMLConstants.INPUT_VARIABLE)
    val operations = keys.filterNot(toRemove.contains(_))

    (inputVariable, operations.toArray)
  }


  private val savePipelineModel: (PipelineModel, Int) => Unit = savePipelineModel(_: PipelineModel, _: Int,
    "vectorization")

  val loadPipelineModel: Int => PipelineModel = loadPipelineModel(_: Int, "vectorization")

  /**
   * Validating flashml Vectorization configuration for checking whether the input variables
   * are part of featuregeneration or preprocessing output variables
   * and checking whether the transformations in the config are supported
   */
  override def validate(): Unit =
  {
    // supported transformations
    val transformations = List[String](
      FlashMLConstants.HASHING_TF,
      FlashMLConstants.COUNT_VECTORIZER,
      FlashMLConstants.WORD2VEC,
      FlashMLConstants.TF_IDF
    )
    // validate if vectorization config is not empty
    if (ConfigUtils.loadTextVectorizationConfig.nonEmpty)
    {
      // Validating based on page level and scope
      if (ConfigUtils.isPageLevelModel)
      {
        if (ConfigUtils.textVectorizationScope.toLowerCase.equals(FlashMLConstants.SCOPE_PARAMETER_NO_PAGE))
        {
          val msg = s"Scope cannot be ${ConfigUtils.textVectorizationScope} for page level model"
          log.error(msg)
          throw new ConfigValidatorException(msg)
        }

        (0 until ConfigUtils.numPages).foreach(pageNumber =>
        {

          // fetching vectorization config steps pagewise
          val textVectorizationSteps: Array[util.HashMap[String, Any]] =
            ConfigUtils.textVectorizationScope.toLowerCase match
            {
              case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
                ConfigUtils.loadTextVectorizationConfig.asInstanceOf[Array[util.HashMap[String, Any]]]

              case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>
                val currTextVectorConfig = ConfigUtils.loadTextVectorizationConfig
                  .asInstanceOf[Array[Array[util.HashMap[String, Any]]]]

                if (currTextVectorConfig.isDefinedAt(pageNumber) && currTextVectorConfig(pageNumber)
                  .nonEmpty)
                  currTextVectorConfig(pageNumber)
                else Array[util.HashMap[String, Any]]()
            }

          // fetching output variables from preprocessing and feature generation config to match them
          // against the
          // input variables of vectorization
          val previousStepOutputVariables = ArrayBuffer[String]()

          if (featureGenerationGramsConfig.nonEmpty)
          {

            val featureGenerationGrams = ConfigUtils.featureGenerationScope match
            {
              case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE
              => featureGenerationGramsConfig
              case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => featureGenerationGramsConfig(pageNumber)
            }

            previousStepOutputVariables ++= featureGenerationGrams
              .asInstanceOf[Array[util.HashMap[String, Any]]]
              .map(_.get(FlashMLConstants.OUTPUT_VARIABLE).toString)
          }

          if (loadPreprocessingConfig.nonEmpty)
          {

            val preprocessingStepsPageWise = ConfigUtils.preprocessingVariablesScope match
            {
              case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
                loadPreprocessingConfig
              case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>
                loadPreprocessingConfig(pageNumber)
            }

            //Filter for checking if output_variable key is present because not all transformations have
            // the key.
            previousStepOutputVariables ++= preprocessingStepsPageWise
              .asInstanceOf[Array[util.HashMap[String, Any]]]
              .filter(_.containsKey(FlashMLConstants.OUTPUT_VARIABLE))
              .map(_.get(FlashMLConstants.OUTPUT_VARIABLE).toString)
          }

          // function call to validate
          variableDependencyValidation(textVectorizationSteps, previousStepOutputVariables.toArray,
            transformations)
        })
      }
      else
      {
        //throwing an error if the scope is perpage or allpage for non page level variables
        ConfigUtils.textVectorizationScope.toLowerCase match
        {
          case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
            val msg = s"Scope cannot be ${ConfigUtils.textVectorizationScope} for non page level model"
            log.error(msg)
            throw new ConfigValidatorException(msg)

          case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>
            // fetching output variables from preprocessing or feature generation config
            val previousStepOutputvariables = ArrayBuffer[String]()

            if (featureGenerationGramsConfig.nonEmpty)
            {
              previousStepOutputvariables ++= featureGenerationGramsConfig
                .asInstanceOf[Array[util.HashMap[String, Any]]]
                .map(_.get(FlashMLConstants.OUTPUT_VARIABLE).toString)
            }

            if (loadPreprocessingConfig.nonEmpty)
            {
              previousStepOutputvariables ++= loadPreprocessingConfig
                .asInstanceOf[Array[util.HashMap[String, Any]]]
                .filter(_.containsKey(FlashMLConstants.OUTPUT_VARIABLE))
                .map(_.get(FlashMLConstants.OUTPUT_VARIABLE).toString)
            }

            // function call to validate
            variableDependencyValidation(ConfigUtils.loadTextVectorizationConfig.asInstanceOf[Array[util.HashMap[String, Any]]], previousStepOutputvariables.toArray, transformations)
        }
      }
    }
  }

  /**
   * Validating whether the vectorization step variable is same as preprocessing or featuregeneration variable
   *
   * @param textVectorizationMapArr     text vectorization Steps
   * @param previousStepOutputvariables List of valid text variables on which vectorization can be applied
   * @param transformations             List of valid transformations
   */
  def variableDependencyValidation(textVectorizationMapArr: Array[util.HashMap[String, Any]], previousStepOutputvariables: Array[String], transformations: List[String]): Unit =
  {
    textVectorizationMapArr.foreach(vect =>
    {
      val inputColumnName = vect.get(FlashMLConstants.INPUT_VARIABLE).asInstanceOf[String]
      val transform = vect.get(FlashMLConstants.VECTORIZATION_TEXT_METHOD).asInstanceOf[String]

      if (!previousStepOutputvariables.contains(inputColumnName))
      {
        val msg = s"$inputColumnName Input Variable is not part of the previous step output variables"
        log.error(msg)
        throw new ConfigValidatorException(msg)
      }

      if (!transformations.contains(transform))
      {
        val msg = s"$transform in Vectorization not supported"
        log.error(msg)
        throw new ConfigValidatorException(msg)
      }

    })
  }

}
