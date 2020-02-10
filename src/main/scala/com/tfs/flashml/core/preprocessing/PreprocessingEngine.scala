package com.tfs.flashml.core.preprocessing

import java.util

import com.tfs.flashml.core.{Engine, Validator}
import com.tfs.flashml.core.VectorizationEngine.log
import com.tfs.flashml.core.preprocessing.transformer._
import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util.conf.{ConfigValidatorException, FlashMLConstants}
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import com.tfs.flashml.util.ConfigUtils.{DataSet, DataSetType}
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.{ImputerCustom, RegexTokenizer, StopWordsRemoverCustom}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Applies Preprocessing transformations on the dataframe.
 * Current Support Includes:
 * Regex Tokenization, Word Substitution, Word Classes Replacement, Sentence Marker, Regex Replacement, Regex
 * Removal,Stemming, Case Normalizaton
 *
 */
object PreprocessingEngine extends Engine with Validator
{

    override val log: Logger = LoggerFactory.getLogger(getClass)

    private case class customArrayHashMap(arrHashMap: Array[util.HashMap[String, Any]])

    private val internalDelimiterPreprocessing = "_"

    val preprocessingMarkers = Map(
        FlashMLConstants.TOKENIZER -> "TKNZR",
        FlashMLConstants.CASE_NORMALIZATON -> "CSNRM",
        FlashMLConstants.CONTRACTIONS_REPLACEMENT -> "CNTRP",
        FlashMLConstants.STEMMING -> "STMM",
        FlashMLConstants.STOPWORDS -> "STPWR",
        FlashMLConstants.SENTENCE_MARKER -> "SNTMR",
        FlashMLConstants.WORD_CLASSES_REPLACEMENT -> "WCRPL",
        FlashMLConstants.REGEX_REPLACEMENT -> "RGXRP",
        FlashMLConstants.REGEX_REMOVAL -> "RGXRM"
    )

    /**
     * Applies preprocessing operations on the dataframe
     *
     * @param odfArray Option - DataFrame Array post sampling
     * @return Option - DataFrame Array post preprocessing
     */
    def process(odfArray: Option[Array[DataFrame]]): Option[Array[DataFrame]] =
    {

        //Returns ArrayBuffer because it is assigned to another object. If the object was an Array,
        //then the size would have to be predefined.

        //First we check what the Preprocessing Scope Variable is
        //Then we convert the parameter accordingly to a Scala type

        odfArray
          .map(dfArray =>
          {

              if (ConfigUtils.isModel)
              {
                  if (ConfigUtils.preprocessingVariablesScope == FlashMLConstants.PREPROCESSING_SCOPE_NONE)
                      return Some(dfArray)
                  else
                  {

                      log.info(s"Preprocessing: Loading config.")

                      if (ConfigUtils.isPageLevelModel)
                      {
                          log.info(s"Preprocessing: Page Level Processing.")

                          (1 to ConfigUtils.numPages)
                            .foreach
                            { pageNumber: Int =>

                                log.info(s"Preprocessing: Building Pipeline for Page $pageNumber.")
                                pipelineModelArray += buildPipelineModel(dfArray(pageNumber - 1),
                                    pageNumber)
                            }

                          dfArray
                            .indices
                            .foreach
                            { index: Int =>
                                val currentDF = pipelineModelArray(index % ConfigUtils.numPages)
                                  .transform(dfArray(index))

                                outputDFArrayBuffer += currentDF
                            }

                          preprocessingSavePointing(isPageLevel = true, outputDFArrayBuffer.toArray)
                      }
                      else
                      {
                          //Non page level model
                          pipelineModelArray += buildPipelineModel(dfArray(0), 0)

                          //For each object fetch all transformation maps and fetch all output column names
                          // except the last
                          dfArray
                            .foreach
                            { df: DataFrame =>
                                val currentDF = pipelineModelArray(0)
                                  .transform(df)
                                outputDFArrayBuffer += currentDF
                            }

                          preprocessingSavePointing(isPageLevel = false, outputDFArrayBuffer.toArray)
                      }
                  }
              }
              else
              {

                  log.info(s"Preprocessing: Loading config.")

                  if (ConfigUtils.isPageLevelModel)
                  {
                      //Page Level Model requires an ArrayBuffer[ArrayBuffer[String]] to denote required
                      // intermediate columns to be dropped
                      (1 to ConfigUtils.numPages).foreach
                      { pageNumber: Int =>
                          pipelineModelArray += loadPipelineModel(pageNumber)

                      }

                      dfArray
                        .indices
                        .foreach
                        { index: Int =>
                            val currentDF = pipelineModelArray(index % ConfigUtils.numPages)
                              .transform(dfArray(index))
                            outputDFArrayBuffer += currentDF
                        }

                      //SAVE
                      preprocessingSavePointing(isPageLevel = true, outputDFArrayBuffer.toArray)
                  }
                  else
                  {
                      //Non Page Level model
                      pipelineModelArray += loadPipelineModel(0)
                      dfArray
                        .foreach
                        { df: DataFrame =>
                            val currentDF = pipelineModelArray(0)
                              .transform(df)

                            outputDFArrayBuffer += currentDF
                        }

                      preprocessingSavePointing(isPageLevel = false, outputDFArrayBuffer.toArray)

                  }
              }
              outputDFArrayBuffer.toArray
          })
    }

    def preprocessingConfigLoader(pageNum: Int) =
    {

        if (ConfigUtils.isPageLevelModel && ConfigUtils.preprocessingVariablesScope == FlashMLConstants
          .SCOPE_PARAMETER_PER_PAGE)
            loadPreprocessingConfig(if (pageNum == 0) 0
            else pageNum - 1).isInstanceOf[customArrayHashMap]

        if (ConfigUtils.preprocessingVariablesScope == FlashMLConstants.SCOPE_PARAMETER_NO_PAGE || ConfigUtils
          .preprocessingVariablesScope == FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE)
            loadPreprocessingConfig
              .asInstanceOf[Array[util.HashMap[String, Any]]]
    }


    /**
     * Read the preprocessing config from the config parameter
     *
     * @return
     */
    def loadPreprocessingConfig = ConfigUtils
      .preprocessingVariablesScope match
    {

        case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
            FlashMLConfig
              .config
              .getList(FlashMLConstants.EXPERIMENT_PREPROCESSING_STEPS)
              .unwrapped()
              .asInstanceOf[java.util.ArrayList[util.HashMap[String, Any]]]
              .asScala
              .toArray
              .asInstanceOf[Array[util.HashMap[String, Any]]]


        case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => FlashMLConfig
          .config
          .getList(FlashMLConstants.EXPERIMENT_PREPROCESSING_STEPS)
          .asScala
          .map(_.unwrapped()
            .asInstanceOf[java.util.ArrayList[util.HashMap[String, Any]]]
            .asScala.toArray)
          .toArray
          .asInstanceOf[Array[Array[util.HashMap[String, Any]]]]

        case _ =>
            log.info(ConfigUtils.scopeProcessingErrorMessage)
            throw new Exception(ConfigUtils.scopeProcessingErrorMessage)
    }

    override def buildPipelineModel(df: DataFrame, pageCount: Int): PipelineModel =
    {

        val preProcessingMapArray =
        {

            ConfigUtils.preprocessingVariablesScope match
            {
                case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>

                    if (ConfigUtils.isPageLevelModel)
                        loadPreprocessingConfig(if (pageCount == 0) 0
                        else pageCount - 1)
                    else
                        loadPreprocessingConfig

                case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
                    loadPreprocessingConfig
            }
        }

        //Holder for storing pipeline stages
        val allStages = ArrayBuffer[PipelineStage]()

        preProcessingMapArray
          .asInstanceOf[Array[util.HashMap[String, Any]]]
          .zipWithIndex
          .foreach(obj =>
          {
              val indexNum = obj._2
              allStages ++= getPipelineStagesForColumn(obj._1, indexNum + 1, pageCount)
          })

        //Build Pipeline
        val preProcessPipeline = new Pipeline()
          .setStages(allStages.toArray)

        //Fit the pipeline of the dataframe and then save
        val preProcessModel: PipelineModel = preProcessPipeline
          .fit(df)
        savePipelineModel(preProcessModel, pageCount)

        preProcessModel
    }

    /**
     * Get the pipeline stages for a particular column. The output column of one transformation
     * becomes the input column of the next transformation. The output column for all the operations
     * except the last one are named "inputColumn_Operation". Output column of the last transformation
     * is the one given in config.
     *
     * @param map Preprocessing Operations for a particular column
     * @return ArrayBuffer containing the PipelineStage
     */
    private def getPipelineStagesForColumn(map: util.HashMap[String, Any], indexValue: Int, pgNum : Int) =
    {

        val inputColumnName = map
          .get(FlashMLConstants.INPUT_VARIABLE)
          .toString

        val outputColName = map
          .getOrDefault(FlashMLConstants.OUTPUT_VARIABLE, "")
          .toString

        val transformations: Array[mutable.Map[String, Any]] = map
          .get(FlashMLConstants.PREPROCESSING_TRANSFORMATIONS)
          .asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
          .asScala
          .map(_.asScala)
          .toArray

        val pageNum = if(pgNum == 0) 0 else pgNum -1

        val tokenizerMap = transformations
          .filter(_ (FlashMLConstants.PREPROCESSING_TYPE).asInstanceOf[String].equals(FlashMLConstants.TOKENIZER))
        val delimiter = if (tokenizerMap.nonEmpty) tokenizerMap(0)
          .getOrElse(FlashMLConstants.PREPROCESSING_METHOD_PARAMETER, "\\s")
          .toString + "|(" + FlashMLConstants.CUSTOM_DELIMITER + ")"
        else ""

        log.info(s"Preprocessing: Processing variable '$inputColumnName'.")


        def getPreprocessingStages(transformations: Array[mutable.Map[String, Any]], inputColumn: String,
                                   outputCol: String) =
        {

            var inputCol = inputColumn
            val transformationsCount = transformations.length

            transformations
              .zipWithIndex
              .foldLeft(ArrayBuffer[PipelineStage]())
              {

                  (pipelineStages, indexedTupleObj) =>

                      val transformationDetail: mutable.Map[String, Any] = indexedTupleObj._1
                      val idx: Int = indexedTupleObj._2

                      val preprocessingType = transformationDetail(FlashMLConstants.PREPROCESSING_TYPE)
                        .toString
                        .toLowerCase

                      log.info(s"Adding $preprocessingType to the pipeline")

                      val newOutputColName =
                          if (idx < transformationsCount - 1)
                          {
                              val intermediateCol =  s"${inputCol}_Col${indexValue}_${preprocessingMarkers.getOrElse(preprocessingType, "")}"

                              ConfigUtils.pagewisePreprocessingIntVariables(pageNum) += intermediateCol
                              intermediateCol
                          }
                          else outputCol

                      val (stage, nextInputColumnName) = preprocessingType match
                      {

                          case FlashMLConstants.TOKENIZER =>

                              val stage: PipelineStage =
                                  new RegexTokenizer()
                                    .setInputCol(inputCol)
                                    .setOutputCol(newOutputColName)
                                    .setPattern(delimiter)
                                    .setToLowercase(false)

                              (stage, newOutputColName)

                          case FlashMLConstants.STEMMING =>

                              val exceptions: Array[String] = PreprocessingStageLoader
                                .getExceptions(transformationDetail(FlashMLConstants
                                  .PREPROCESSING_METHOD_PARAMETER))

                              val stage: PipelineStage = new PorterStemmingTransformer()
                                .setInputCol(inputCol)
                                .setOutputCol(newOutputColName)
                                .setExceptions(exceptions)
                                .setDelimiter(delimiter)

                              (stage, newOutputColName)

                          case FlashMLConstants.CONTRACTIONS_REPLACEMENT | FlashMLConstants.LEMMATIZE =>

                              val dictionary: Map[String, String] = PreprocessingStageLoader
                                .getWordReplacements(transformationDetail(FlashMLConstants
                                  .PREPROCESSING_METHOD_PARAMETER))

                              val stage: PipelineStage = new WordSubstitutionTransformer()
                                .setInputCol(inputCol)
                                .setOutputCol(newOutputColName)
                                .setDictionary(dictionary)
                                .setDelimiter(delimiter)

                              (stage, newOutputColName)

                          case FlashMLConstants.SENTENCE_MARKER =>

                              val stage: PipelineStage = new SentenceMarker()
                                .setInputCol(inputCol)
                                .setOutputCol(newOutputColName)

                              (stage, newOutputColName)

                          case FlashMLConstants.STOPWORDS =>

                              val stopWords: Array[String] = PreprocessingStageLoader
                                .getStopWords(transformationDetail(FlashMLConstants
                                  .PREPROCESSING_METHOD_PARAMETER))

                              val stage: PipelineStage = new StopWordsRemoverCustom()
                                .setInputCol(inputCol)
                                .setOutputCol(newOutputColName)
                                .setStopWords(stopWords)
                                .setDelimiter(delimiter)

                              (stage, newOutputColName)

                          case FlashMLConstants.NULL_CHECK =>

                              val replaceValue = transformationDetail(FlashMLConstants
                                .PREPROCESSING_METHOD_PARAMETER)
                                .toString

                              val stage: PipelineStage = new ImputerCustom()
                                .setInputColumn(inputCol)
                                .setReplacementValue(replaceValue)
                              // This is an inplace transformation, so we return the input column.
                              (stage, inputCol)

                          case FlashMLConstants.WORD_CLASSES_REPLACEMENT =>

                              val wordClasses: Seq[(String, String)] = PreprocessingStageLoader
                                .getWordClassReplacements(transformationDetail(FlashMLConstants
                                  .PREPROCESSING_METHOD_PARAMETER))
                                .sortBy(-_._1.length)
                              val wordRegexSeq = PreprocessingStageLoader.computeWordRegexSeq(wordClasses)

                              val stage: PipelineStage = new RegexReplacementTransformer()
                                .setInputCol(inputCol)
                                .setOutputCol(newOutputColName)
                                .setRegexReplacements(wordRegexSeq)

                              (stage, newOutputColName)

                          case FlashMLConstants.REGEX_REMOVAL =>
                              val regexClasses: Seq[(String, String)] = PreprocessingStageLoader
                                .getRegexRemovalPatterns(transformationDetail(FlashMLConstants
                                  .PREPROCESSING_METHOD_PARAMETER))

                              val stage: PipelineStage = new RegexReplacementTransformer()
                                .setInputCol(inputCol)
                                .setOutputCol(newOutputColName)
                                .setRegexReplacements(regexClasses)
                                .setDelimiter(delimiter)

                              (stage, newOutputColName)
                          case FlashMLConstants.REGEX_REPLACEMENT =>
                              val regexClasses: Seq[(String, String)] = PreprocessingStageLoader
                                .getRegexes(transformationDetail(FlashMLConstants
                                  .PREPROCESSING_METHOD_PARAMETER))

                              val stage: PipelineStage = new RegexReplacementTransformer()
                                .setInputCol(inputCol)
                                .setOutputCol(newOutputColName)
                                .setRegexReplacements(regexClasses)
                                .setDelimiter(delimiter)

                              (stage, newOutputColName)
                          case FlashMLConstants.CASE_NORMALIZATON =>

                              val stage: PipelineStage = new CaseNormalizationTransformer()
                                .setInputCol(inputCol)
                                .setOutputCol(newOutputColName)

                              (stage, newOutputColName)
                          case _ => throwException("Unidentified Preprocessing Method chosen! Please select a " +
                            "valid Preprocessing method and try again.")
                      }

                      //Setting up for the next iteration
                      if (idx != transformationsCount - 1)
                          inputCol = nextInputColumnName

                      pipelineStages += stage
              }
              .toArray

        }

        getPreprocessingStages(transformations, inputColumnName, outputColName)
    }

    private def throwException(msg: String) =
    {
        log.error(msg)
        throw new SparkException(msg)
    }

    val savePipelineModel: (PipelineModel, Int) => Unit = savePipelineModel(_: PipelineModel, _: Int,
        FlashMLConstants.PREPROCESSING)

    val loadPipelineModel: Int => PipelineModel = loadPipelineModel(_: Int, FlashMLConstants.PREPROCESSING)


    /**
     * Validating flashml preprocessing configuration for checking whether the input variable is part of text or
     * numerical or categorical variables
     * Also validating whether the transformations present in config are supported.
     */
    override def validate(): Unit =
    {
        // List of supported transformations
        val transformations = List[String](
            FlashMLConstants.TOKENIZER,
            FlashMLConstants.CASE_NORMALIZATON,
            FlashMLConstants.CONTRACTIONS_REPLACEMENT,
            FlashMLConstants.LEMMATIZE,
            FlashMLConstants.STEMMING,
            FlashMLConstants.STOPWORDS,
            FlashMLConstants.SENTENCE_MARKER,
            FlashMLConstants.NULL_CHECK,
            FlashMLConstants.WORD_CLASSES_REPLACEMENT,
            FlashMLConstants.REGEX_REPLACEMENT,
            FlashMLConstants.REGEX_REMOVAL
        )
        // loading the preprocessing steps
        val preprocessingSteps = loadPreprocessingConfig
        // Validating based on page level and scope
        if (ConfigUtils.isPageLevelModel)
            (0 until ConfigUtils.numPages)
              .foreach(pageNumber =>
              {
                  // fetching the text variables
                  val textVariables = ConfigUtils.variablesScope match
                  {
                      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => ConfigUtils.scopeTextVariables
                      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => ConfigUtils.scopeTextVariables(pageNumber)
                  }
                  // fetching the categorical variables
                  val categoricalVariables = ConfigUtils.variablesScope match
                  {
                      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => ConfigUtils.scopeCategoricalVariables
                      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => ConfigUtils.scopeCategoricalVariables(pageNumber)
                  }
                  // fetching numerical variables
                  val numericalVariables = ConfigUtils.variablesScope match
                  {
                      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => ConfigUtils.scopeNumericalVariables
                      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => ConfigUtils.scopeNumericalVariables(pageNumber)
                  }

                  var variableUnion = textVariables.asInstanceOf[Array[String]].toSet ++ numericalVariables.asInstanceOf[Array[String]].toSet ++ categoricalVariables.asInstanceOf[Array[String]].toSet

                  // fetching preprocessing steps based on preprocessing scope
                  val preprocessingStepsPageWise = ConfigUtils.preprocessingVariablesScope match
                  {
                      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => preprocessingSteps(pageNumber)
                      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => preprocessingSteps
                  }
                  // functions call to validate
                  variableDependencyValidation(preprocessingStepsPageWise.asInstanceOf[Array[util.HashMap[String, Any]]], variableUnion.asInstanceOf[Set[String]], transformations)
              })
        else
        {
            //throwing an error if the scope is perpage or allpage for non page level variables
            ConfigUtils.preprocessingVariablesScope match
            {
                case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
                    val msg = s"Scope cannot be ${ConfigUtils.preprocessingVariablesScope} for non page level model"
                    log.error(msg)
                    throw new ConfigValidatorException(msg)
                case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>
                    var variableUnion = ConfigUtils.scopeTextVariables.asInstanceOf[Array[String]].toSet ++ ConfigUtils.scopeNumericalVariables.asInstanceOf[Array[String]].toSet ++ ConfigUtils.scopeCategoricalVariables.asInstanceOf[Array[String]].toSet
                    variableDependencyValidation(preprocessingSteps.asInstanceOf[Array[util.HashMap[String, Any]]], variableUnion.asInstanceOf[Set[String]], transformations)
            }
        }
    }

    /**
     * Validating whether the preprocessing steps input variable is part of text, numerical or categorical variables and whether the transformations are valid
     *
     *
     *
     * @param preprocessingSteps Preprocessing Step array
     * @param variableUnion      List of valid text variables on which preprocessing can be applied
     * @param transformations    List of valid transformations
     */
    def variableDependencyValidation(preprocessingSteps: Array[util.HashMap[String, Any]], variableUnion: Set[String], transformations: List[String]): Unit =
    {
        //iterating through preprocessing steps and validating the input variables and transformations
        preprocessingSteps
          .asInstanceOf[Array[util.HashMap[String, Any]]]
          .zipWithIndex
          .foreach(step =>
          {
              val inputvar = step._1
                .get(FlashMLConstants.INPUT_VARIABLE)
                .toString

              if (!variableUnion.contains(inputvar))
              {
                  val msg = s"$inputvar Input Variable is not part of the text variables"
                  log.error(msg)
                  throw new ConfigValidatorException(msg)
              }
              step._1
                .get(FlashMLConstants.PREPROCESSING_TRANSFORMATIONS)
                .asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
                .asScala
                .map(_.asScala)
                .foreach(transform =>
                {
                    if (!transformations.contains(transform(FlashMLConstants.PREPROCESSING_TYPE)))
                    {
                        val msg = s"Transformation ${transform(FlashMLConstants.PREPROCESSING_TYPE)} is not supported"
                        log.error(msg)
                        throw new ConfigValidatorException(msg)
                    }
                })
          })
    }

    def preprocessingSavePointing(isPageLevel: Boolean, dfArray: Array[DataFrame]): Unit =
    {

        if (isPageLevel)
        {
            dfArray
              .indices
              .foreach(x =>
              {
                  SavePointManager
                    .saveDataFrame(dfArray(x), x % ConfigUtils.numPages + 1, DataSetType(x / ConfigUtils.numPages), FlashMLConstants.PREPROCESSING)
              })
        }
        else
        {
            dfArray
              .indices
              .foreach(x =>
              {
                  SavePointManager
                    .saveDataFrame(dfArray(x), pageCount = 0, DataSetType(x), FlashMLConstants.PREPROCESSING)
              })
        }
    }

    /**
     * Based on input Scope option, we have to parse through a generic object
     * In this case the generic object is a HashMap representing series of transformations made to a single variable
     * Internally, the preprocessing intermediate columns are generated and then stored in a global variable for further use
     * The reason for computing the intermediate columns is to optimize caching when selecting only required columns
     * In the case of loading Preprocessed Data, these intermediate columns will be generated again to ensure optimization while caching data
     */
    def populatePreprocessingIntermediateColumnNames =
    {

        def computeIntermediateVariables(map : util.HashMap[String,Any], index : Int, pgNum: Int): Unit ={

            var inputColumnName = map
              .get(FlashMLConstants.INPUT_VARIABLE)
              .toString

            val outputColName = map
              .get(FlashMLConstants.OUTPUT_VARIABLE)
              .toString

            val transformations = map.get(FlashMLConstants.PREPROCESSING_TRANSFORMATIONS)
              .asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala
              .map(_.asScala)
              .toArray

            val transformationsCount = transformations.length

            transformations
              .zipWithIndex
              .foreach(
                  obj => {

                      val preprocessingType = obj._1(FlashMLConstants.PREPROCESSING_TYPE)
                        .toString
                        .toLowerCase

                      val idx = obj._2

                      val newOutputColName =
                          if (idx < transformationsCount - 1)
                          {
                              val intermediateCol =  s"${inputColumnName}_Col${index}_${preprocessingMarkers.getOrElse(preprocessingType, "")}"

                              ConfigUtils.pagewisePreprocessingIntVariables(pgNum) += intermediateCol
                              intermediateCol
                          }
                          else outputColName

                  })

        }

        FlashMLConfig
          .getString(FlashMLConstants.PREPROCESSING_SCOPE)
          .toLowerCase match {

            case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>
                loadPreprocessingConfig
                  .asInstanceOf[Array[util.HashMap[String,Any]]]
                  .zipWithIndex
                  .foreach(obj=>{
                      val indx = obj._2
                      computeIntermediateVariables(obj._1, indx, 0)
                  })

            case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>
                loadPreprocessingConfig
                  .asInstanceOf[Array[Array[util.HashMap[String,Any]]]]
                  .zipWithIndex
                  .foreach(pagewiseObj =>
                  {
                      //ZipWithIndex ensures indexing starts from zero
                      val pgNum = pagewiseObj._2
                      val pgWiseTransformations = pagewiseObj._1
                      pgWiseTransformations
                        .zipWithIndex
                        .foreach(obj => {
                            val colIndx = obj._2
                            computeIntermediateVariables(obj._1, colIndx, pgNum)
                        })

                  })

            case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => val preprocessingConfig = loadPreprocessingConfig
              .asInstanceOf[Array[util.HashMap[String,Any]]]
                (0 until ConfigUtils.numPages)
                  .foreach(pGindx => {
                      preprocessingConfig
                        .zipWithIndex
                        .foreach(obj => {
                            val colIndx = obj._2
                            computeIntermediateVariables(obj._1,colIndx, pGindx)
                        })
                  })

        }

    }

}
