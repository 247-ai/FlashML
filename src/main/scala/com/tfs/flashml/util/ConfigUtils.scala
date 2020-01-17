package com.tfs.flashml.util

import java.util

import com.tfs.flashml.core.ModelTrainingEngine.algorithm

//import com.tfs.flashml.core.featuregeneration.FeatureGenerationEngine.featureGenerationBinningConfig

//import com.tfs.flashml.util.ConfigUtils.{binningConfigPageVariables, featureGenerationBinningConfig, featureGenerationConfig}
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


/** Note: All variables whose values are extracted from config parameter
  * need to be lazy val. This is because the config parameter Path is set in the main
  * and is "" by default.
  *
  */
object ConfigUtils {

  private val log = LoggerFactory.getLogger(getClass)
  var configFilePath = ""

  lazy val hdfsNameNodeURI: String = FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI)
  lazy val fs: FileSystem = {
    val hdfsConf = new Configuration()
    hdfsConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hdfsConf.set("fs.parameter.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    hdfsConf.set("fs.defaultFS", hdfsNameNodeURI)
    FileSystem.get(hdfsConf)
  }

  lazy val responseColumn: String = FlashMLConfig
    .getString(FlashMLConstants.RESPONSE_VARIABLE)

  def getIndexedResponseColumn: String = responseColumn + FlashMLConstants.INDEXED

  lazy val modelingMethod: Array[String] = FlashMLConfig.getStringArray(FlashMLConstants.EXPERIMENT_MODELING_METHOD)
    .filter(!_.isEmpty)
    .distinct
    .map(_.toLowerCase)

  lazy val experimentType: String = FlashMLConfig.getString(FlashMLConstants.EXPERIMENT_TYPE)

  lazy val isModel: Boolean = experimentType == FlashMLConstants.EXPERIMENT_TYPE_MODEL

  lazy val isPredict: Boolean = experimentType == FlashMLConstants.EXPERIMENT_TYPE_PREDICT

  lazy val isSingleIntent: Boolean = modelingMethod.contains(FlashMLConstants.EXPERIMENT_MODELING_METHOD_SINGLE_INTENT)

  lazy val isMultiIntent: Boolean = modelingMethod.contains(FlashMLConstants.EXPERIMENT_MODELING_METHOD_MULTI_INTENT)

  lazy val isUplift: Boolean = modelingMethod.contains(FlashMLConstants.UPLIFT)
  lazy val upliftColumn: String = if (isUplift) FlashMLConfig.getString(FlashMLConstants.UPLIFT_VARIABLE) else ""

  lazy val variablesScope = FlashMLConfig
    .getString(FlashMLConstants.VARIABLES_SCOPE)
    .toLowerCase

  lazy val scopeTextVariables = variablesScope match {

    case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => FlashMLConfig.getStringArray(FlashMLConstants.VARIABLES_TEXT_COL)

    case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => FlashMLConfig.get2DStringArray(FlashMLConstants.VARIABLES_TEXT_COL)
  }

  lazy val scopeTextVariables1DArray = scopeTextVariables.asInstanceOf[Array[String]]

  lazy val scopeTextVariables2DArray = scopeTextVariables.asInstanceOf[Array[Array[String]]]

  lazy val scopeCategoricalVariables = variablesScope match {
    case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_NO_PAGE => FlashMLConfig
      .getStringArray(FlashMLConstants.VARIABLES_CATEGORICAL_COL)

    case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => FlashMLConfig
      .get2DStringArray(FlashMLConstants.VARIABLES_CATEGORICAL_COL)
  }

  lazy val scopeCategoricalVariables1DArray = scopeCategoricalVariables.asInstanceOf[Array[String]]

  lazy val scopeCategoricalVariables2DArray = scopeCategoricalVariables.asInstanceOf[Array[Array[String]]]

  lazy val scopeNumericalVariables = variablesScope match {
    case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_NO_PAGE => FlashMLConfig
      .getStringArray(FlashMLConstants.VARIABLES_NUMERICAL_COL)
      .filterNot(_.isEmpty)
      .distinct

    case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => FlashMLConfig
      .get2DStringArray(FlashMLConstants.VARIABLES_NUMERICAL_COL)
      .map(_.filterNot(_.isEmpty)
        .distinct)
  }

  // Numerical columns are bound to be updated based on Feature Generation scope option submitted by user
  // With the presence of binning option, numerical columns are moved to categorical columns with the label "binned"
  // However in the case of Publish or QA Data Generation it is essential that the loaded variables are entered, rather than the modified ones

  lazy val numericalColumns = {
    if (!FlashMLConfig.hasKey(FlashMLConstants.EXPERIMENT_FEATURE_GENERATION_BINNING) || ConfigUtils.binningConfigPageVariables.isEmpty) {
      ConfigUtils.scopeNumericalVariables
    }
    else {

      val featureGenScope = FlashMLConfig.getString(FlashMLConstants.EXPT_FEATURE_GENERATION_SCOPE).toLowerCase

      variablesScope match {

        case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
          //Supporting only All Page and Per Page

          featureGenScope match {

            case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>

              // We filter out the numerical columns that appear in binning
              //the numerical columns mentioned in binning are moved to categorical variables
              binningConfigPageVariables
                .asInstanceOf[Array[Array[(String, String)]]]
                .zipWithIndex
                .map({
                  case (obj, index) => {
                    ConfigUtils.scopeNumericalVariables.asInstanceOf[Array[String]]
                      .filterNot(obj.map(_._1).toSet)
                  }
                })

            case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>
              throw new Exception(s"For AllPage variable scope option, the only permitted Feature Generation scope options are AllPage and PerPage")

          }

        case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>

          //Support only NoPage feature Generation Scope parameter
          featureGenScope match {

            case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>
              ConfigUtils
                .scopeNumericalVariables
                .asInstanceOf[Array[String]]
                .filterNot(binningConfigPageVariables
                  .asInstanceOf[Array[(String, String)]]
                  .map(_._1).toSet)

            case _ =>
              throw new Exception(s"with NoPage variable scope option, the only feature Generation permitted scope option is NoPage")
          }

        case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>
          //Support only PerPage and AllPage feature Generation Scope parameter

          featureGenScope match {
            case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>

              ConfigUtils
                .scopeNumericalVariables
                .indices
                .map(pageIndex => ConfigUtils
                  .scopeNumericalVariables(pageIndex)
                  .asInstanceOf[Array[String]]
                  .filterNot(binningConfigPageVariables(pageIndex)
                    .asInstanceOf[Array[(String, String)]]
                    .map(_._1)
                    .toSet))
                .toArray

            case _ =>
              throw new Exception(s"With PerPage Variable scope option, the only Feature Generation scope permitted options are PerPage and AllPage")
          }
      }
    }
  }

  lazy val numericalColumns1DArray = numericalColumns.asInstanceOf[Array[String]]

  lazy val numericalColumns2DArray = numericalColumns.asInstanceOf[Array[Array[String]]]

  // User entered categorical column values are received in the variable name scopeCategoricalVariables
  // Based on variable declaration scope option,categorical columns could be 1D or 2D array of strings
  // Furthermore, categorical variables need to be updated based on Binning parameters entered in Feature Generation config
  // Only certain options of Feature Generation scope are supported, based on variable declaration scope option selected by the user
  // Due to binning, the numerical columns are moved to Categorical columns array with the label "binned"

  lazy val categoricalColumns = {
    if (!FlashMLConfig.hasKey(FlashMLConstants.EXPERIMENT_FEATURE_GENERATION_BINNING) || binningConfigPageVariables.isEmpty) {
      scopeCategoricalVariables
    }
    else {

      val featureGenScope = FlashMLConfig
        .getString(FlashMLConstants.EXPT_FEATURE_GENERATION_SCOPE)
        .toLowerCase

      variablesScope match {

        case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => {

          //In the case of All Page Variables' Scope
          //FlashML supports only All Page and PerPage of Feature Generation Scope
          featureGenScope match {

            case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>

              ConfigUtils
                .binningConfigPageVariables
                .asInstanceOf[Array[Array[(String, String)]]]
                .zipWithIndex
                .map(
                  { case (obj, index) => {
                    if (scopeCategoricalVariables.isEmpty)
                      obj.map(_._2)
                    else
                      obj.map(_._2) ++ scopeCategoricalVariables1DArray

                  }
                  }
                )

            case _ =>  throw new Exception(s"For AllPage variable scope option, the only permitted Feature Generation scope options are 'allpage' and PerPage")
          }
        }

        case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE => {
          //Accept only No Page Feature Generation Scope option

          featureGenScope match {
            case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>

              val currentPageLevelCategoricalCol: Array[String] = ConfigUtils
                .binningConfigPageVariables
                .asInstanceOf[Array[(String, String)]]
                .map(_._2)

              if (ConfigUtils.scopeCategoricalVariables.nonEmpty) {
                ConfigUtils.scopeCategoricalVariables1DArray ++ currentPageLevelCategoricalCol
              }
              else
                currentPageLevelCategoricalCol

            case _ => throw new Exception(s"With NoPage variable scope option, the only feature Generation permitted scope option is NoPage")

          }
        }

        case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => {
          //Support only PerPage and AllPage Binning Options

          featureGenScope match {

            case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>

              binningConfigPageVariables
                .asInstanceOf[Array[Array[(String, String)]]]
                .zipWithIndex
                .map({
                  case (obj, index) => {
                    if (scopeCategoricalVariables2DArray.isDefinedAt(index)) {
                      scopeCategoricalVariables2DArray(index) ++ obj.map(_._2)
                    }
                    else obj.map(_._2)
                  }
                })


            case _ =>
              throw new Exception(s"With PerPage Variable scope option, the only Feature Generation scope permitted options are PerPage and AllPage")
          }

        }

        case _ =>
          throw new Exception(ConfigUtils.scopeProcessingErrorMessage)
      }
    }
  }

  lazy val isTopKPossible = ConfigUtils.isMultiIntent && !ConfigUtils.topKIntentColumnName.isEmpty &&
          (FlashMLConstants.probabilitySupportedAlgorithms.contains(mlAlgorithm) || isPlattScalingReqd || mlAlgorithm == FlashMLConstants.LOGISTIC_REGRESSION)

  lazy val isProbabilityColGenerated = FlashMLConstants.probabilitySupportedAlgorithms.contains(mlAlgorithm) || isPlattScalingReqd

  lazy val categoricalColumns2DArray = categoricalColumns.asInstanceOf[Array[Array[String]]]

  lazy val categoricalColumns1DArray = categoricalColumns.asInstanceOf[Array[String]]

  lazy val additionalColumns: Array[String] = FlashMLConfig
    .getStringArray(FlashMLConstants.ADDITIONAL_VARIABLES)
    .filter(!_.isEmpty)
    .distinct

  lazy val primaryKeyColumns: Array[String] = FlashMLConfig
    .getStringArray(FlashMLConstants.PRIMARY_KEY)
    .filter(!_.isEmpty)
    .distinct

  lazy val featureGenerationConfig: util.HashMap[String, Any] = FlashMLConfig
    .config
    .getAnyRef(FlashMLConstants.EXPERIMENT_FEATURE_GENERATION)
    .asInstanceOf[java.util.HashMap[String, Any]]

  lazy val preprocessingVariablesScope = FlashMLConfig
    .getString(FlashMLConstants.PREPROCESSING_SCOPE)
    .toLowerCase

  lazy val featureGenerationScope = FlashMLConfig
    .getString(FlashMLConstants.EXPT_FEATURE_GENERATION_SCOPE)
    .toLowerCase

  lazy val featureGenerationGramsConfig =
    featureGenerationScope
    match {
      case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>

        featureGenerationConfig(FlashMLConstants.FEATURE_GENERATION_GRAMS)
          .asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
          .asScala
          .toArray

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>

        featureGenerationConfig(FlashMLConstants.FEATURE_GENERATION_GRAMS)
          .asInstanceOf[java.util.ArrayList[java.util.ArrayList[java.util.HashMap[String, Any]]]]
          .asScala
          .map(_.asScala.toArray)
          .toArray
    }

  lazy val featureGenerationBinningConfig =

    FlashMLConfig
      .getString(FlashMLConstants.EXPT_FEATURE_GENERATION_SCOPE)
      .toLowerCase match {

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>

        featureGenerationConfig(FlashMLConstants.FEATURE_GENERATION_BINNING)
          .asInstanceOf[java.util.ArrayList[java.util.ArrayList[java.util.HashMap[String, Any]]]]
          .asScala
          .map(_.asScala.toArray)
          .toArray

      case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>

        featureGenerationConfig(FlashMLConstants.FEATURE_GENERATION_BINNING)
          .asInstanceOf[java.util.ArrayList[util.HashMap[String, Any]]]
          .asScala
          .toArray
    }

  // On a page level, based on Binning options entered by user in config file, FlashML builds Array of Tuples on each page
  // The numerical column is marked as first variable in tuple, second value is auto-generated binned column name
  // 2D Array is thus computed and will be used to update list of categorical and numerical columns entered by user

  lazy val binningConfigPageVariables = {
    if (featureGenerationBinningConfig
      .asInstanceOf[Array[Any]]
      .nonEmpty) {
      // Will return a 2D array denoting multi page level traversal in case of perPage & allPage
      FlashMLConfig
        .getString(FlashMLConstants.EXPT_FEATURE_GENERATION_SCOPE)
        .toLowerCase match {

        case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>
          //supported only for noPage Categorical Variables Declaration
          featureGenerationBinningConfig
            .asInstanceOf[Array[util.HashMap[String, Any]]]
            .map(l2Map => {
              (l2Map(FlashMLConstants.INPUT_VARIABLE).toString, l2Map(FlashMLConstants.INPUT_VARIABLE).toString + "_page1_binned")
            })

        case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => {
          featureGenerationBinningConfig
            .asInstanceOf[Array[Array[util.HashMap[String, Any]]]]
            .zipWithIndex
            .map(
              { case (obj, index) => {
                obj
                  .map(l2Map => {
                    (l2Map(FlashMLConstants.INPUT_VARIABLE).toString, l2Map(FlashMLConstants.INPUT_VARIABLE).toString + "_page" + (index + 1) + "_binned")
                  }
                  )
              }
              }
            )
        }

        case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
          //Building 2D Array of binning Config variables

          (0 until ConfigUtils.numPages)
            .map(index => {
              featureGenerationBinningConfig
                .asInstanceOf[Array[util.HashMap[String, Any]]]
                .map(l2Map => {
                  (l2Map(FlashMLConstants.INPUT_VARIABLE).toString, l2Map(FlashMLConstants.INPUT_VARIABLE).toString + "_page" + (index + 1) + "_binned")
                })
            }).toArray
      }
    }
    else Array()
  }

  //Get the variables to be passed to the vector assembler
  lazy val getColumnsToAssemble: Array[Array[String]] = {

    val columns: Array[ArrayBuffer[String]] = Array
      .fill[ArrayBuffer[String]](if (!isPageLevelModel) 1 else numPages)(ArrayBuffer[String]())

    if (loadTextVectorizationConfig
      .asInstanceOf[Array[Any]]
      .nonEmpty) {

      ConfigUtils
        .textVectorizationScope
        .toLowerCase match {

        case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>

          columns(0) ++=
            ConfigUtils.
              loadTextVectorizationConfig
              .asInstanceOf[Array[util.HashMap[String, Any]]]
              .map(vectorizationMap => {
                  vectorizationMap.get(FlashMLConstants.INPUT_VARIABLE)+ "_" + vectorizationMap.get(FlashMLConstants.VECTORIZATION_TEXT_METHOD).asInstanceOf[String]
              })

        case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>

          loadTextVectorizationConfig
            .asInstanceOf[Array[Array[util.HashMap[String, Any]]]]
            .zipWithIndex
            .foreach { case (obj, indx) =>

              columns(indx) ++= obj
                .map(vectorizationMap => {

                  vectorizationMap.get(FlashMLConstants.INPUT_VARIABLE)+ "_" + vectorizationMap.get(FlashMLConstants.VECTORIZATION_TEXT_METHOD).asInstanceOf[String]
                }
                )
            }

        case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>

          val allPageTextVectorizationOutputCols: Array[String] =
            loadTextVectorizationConfig
              .asInstanceOf[Array[util.HashMap[String, Any]]]
              .map({ vectorizationMap =>

                vectorizationMap.get(FlashMLConstants.INPUT_VARIABLE)+ "_" + vectorizationMap.get(FlashMLConstants.VECTORIZATION_TEXT_METHOD).asInstanceOf[String]

              })

          columns
            .indices
            .foreach(indx => {
              columns(indx) ++= allPageTextVectorizationOutputCols

            })
      }
    }

    ConfigUtils.variablesScope match {

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => {
        for (x <- columns.indices) {
          if (categoricalColumns2DArray.isDefinedAt(x) && categoricalColumns2DArray(x).nonEmpty)
            columns(x) += FlashMLConstants.CATEGORICAL_ARRAY + "_" + FlashMLConfig.getString(FlashMLConstants.CATEGORICAL_VARIABLES_VECTORIZATION_METHOD)

          if (numericalColumns2DArray.isDefinedAt(x) && numericalColumns2DArray(x).nonEmpty)
            columns(x) ++= numericalColumns2DArray(x)

          if (isUplift)
            columns(x) += upliftColumn
        }
        columns.map(_.toArray)

      }

      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => {
        // On a page level, we collect all the numerical and categorical variables
        // Categorical columns although entered by the user, may be empty based on Binning parameters entered by the user

        columns
          .indices
          .foreach(index => {
            if (ConfigUtils.numPages > 1 && FlashMLConfig.hasKey(FlashMLConstants.EXPERIMENT_FEATURE_GENERATION_BINNING) && binningConfigPageVariables.nonEmpty) {
              if (categoricalColumns2DArray.nonEmpty)
                columns(index) += FlashMLConstants.CATEGORICAL_ARRAY + "_" + FlashMLConfig.getString(FlashMLConstants.CATEGORICAL_VARIABLES_VECTORIZATION_METHOD)

              if (numericalColumns2DArray.nonEmpty)
                columns(index) ++= numericalColumns2DArray(index)
            }
            else {

              if (categoricalColumns1DArray.nonEmpty)
                columns(index) += FlashMLConstants.CATEGORICAL_ARRAY + "_" + FlashMLConfig.getString(FlashMLConstants.CATEGORICAL_VARIABLES_VECTORIZATION_METHOD)

              if (numericalColumns1DArray.nonEmpty)
                columns(index) ++= numericalColumns1DArray

            }
            if (isUplift)
              columns(index) += upliftColumn
        })

        columns
          .map(_.toArray)
      }
      case _ => throw new Exception(s"Invalid Scope Option selected!")
    }
  }

  lazy val topVariable: String = FlashMLConfig.getString(FlashMLConstants.TOP_VARIABLE)

  lazy val pageColumn: String = FlashMLConfig.getString(FlashMLConstants.PAGE_VARIABLE)

  lazy val processedCategoricalColumns = new ArrayBuffer[Array[String]]()

  lazy val cumulativeSessionTime = FlashMLConfig.getString(FlashMLConstants.CUMULATIVE_SESSION_TIME)
  lazy val topOrCumSessionTime = if (cumulativeSessionTime.isEmpty) topVariable else cumulativeSessionTime

  lazy val allNumericVariables: Array[String] = {

    ConfigUtils.variablesScope match {

      case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>

        if (FlashMLConfig.hasKey(FlashMLConstants.EXPERIMENT_FEATURE_GENERATION_BINNING) && binningConfigPageVariables.nonEmpty)
          numericalColumns2DArray
            .flatten
            .distinct

        else numericalColumns1DArray

      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE => numericalColumns1DArray

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => numericalColumns2DArray.flatten.distinct

    }
  }

  lazy val allCategoricalVariables: Array[String] = {

    ConfigUtils.variablesScope match {
      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => categoricalColumns1DArray

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => categoricalColumns2DArray.flatten.distinct
    }
  }

  lazy val allTextVariables: Array[String] = {
    variablesScope match {

      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
        scopeTextVariables1DArray

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>
        scopeTextVariables2DArray
        .flatten
        .distinct
        .filterNot(_.isEmpty)
    }
  }

  lazy val randomVariable1: String = FlashMLConfig.getString(FlashMLConstants.RANDOM_VARIABLE)
  lazy val dateVariable: String = FlashMLConfig.getString(FlashMLConstants.DATE_VARIABLE)
  lazy val randomNumGenVariable: String = FlashMLConfig.getString(FlashMLConstants.RANDOM_NUMBER_GENEARATOR_VARIABLE)

  lazy val singleValuesVariables: Array[String] = Array(randomVariable1, randomNumGenVariable, topOrCumSessionTime, pageColumn, responseColumn, upliftColumn, dateVariable)

  lazy val textVectorizationScope = FlashMLConfig.getString(FlashMLConstants.EXPERIMENT_TEXT_VECTORIZATION_SCOPE)

  lazy val columnNamesVariables: Array[String] = {
    val numericalAndCategoricalVariables = ConfigUtils.variablesScope match {

      case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_NO_PAGE => originalCategoricalColumns1DArray ++ originalNumericalColumns1DArray

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => originalNumericalColumns2DArray.flatten.distinct ++ originalCategoricalColumns2DArray.flatten.distinct
    }

    (primaryKeyColumns ++ additionalColumns ++ allTextVariables ++ numericalAndCategoricalVariables ++ singleValuesVariables).filterNot(_.isEmpty)
  }

  lazy val originalNumericalColumns = {
    ConfigUtils.variablesScope match {
      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => FlashMLConfig.getStringArray(FlashMLConstants.VARIABLES_NUMERICAL_COL)
      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => FlashMLConfig.get2DStringArray(FlashMLConstants.VARIABLES_NUMERICAL_COL)
    }
  }

  lazy val originalCategoricalColumns = {
    ConfigUtils.variablesScope match {
      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => FlashMLConfig.getStringArray(FlashMLConstants.VARIABLES_CATEGORICAL_COL)
      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => FlashMLConfig.get2DStringArray(FlashMLConstants.VARIABLES_CATEGORICAL_COL)
    }
  }

  lazy val originalNumericalColumns1DArray: Array[String] = originalNumericalColumns.asInstanceOf[Array[String]]

  lazy val originalNumericalColumns2DArray: Array[Array[String]] = originalNumericalColumns.asInstanceOf[Array[Array[String]]]

  lazy val originalCategoricalColumns1DArray: Array[String] = originalCategoricalColumns.asInstanceOf[Array[String]]

  lazy val originalCategoricalColumns2DArray: Array[Array[String]] = originalCategoricalColumns.asInstanceOf[Array[Array[String]]]

  lazy val loadTextVectorizationConfig = {

    FlashMLConfig.getString(FlashMLConstants.EXPERIMENT_TEXT_VECTORIZATION_SCOPE)
      .toLowerCase match {
      case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>

        FlashMLConfig
          .config
          .getList(FlashMLConstants.EXPERIMENT_TEXT_VECTORIZATION_STEPS)
          .unwrapped()
          .asInstanceOf[java.util.ArrayList[util.HashMap[String, Any]]]
          .asScala
          .toArray
          .asInstanceOf[Array[util.HashMap[String, Any]]]

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>
        FlashMLConfig
          .config
          .getList(FlashMLConstants.EXPERIMENT_TEXT_VECTORIZATION_STEPS)
          .asScala
          .map(_.unwrapped()
            .asInstanceOf[java.util.ArrayList[util.HashMap[String, Any]]]
            .asScala.toArray)
          .to[Array]
    }
  }

  lazy val columnVariables: Array[String] = {
    val pageVariables = ConfigUtils.variablesScope match {

      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>
        (scopeNumericalVariables.asInstanceOf[Array[String]] ++ scopeCategoricalVariables1DArray ++ scopeTextVariables1DArray)
                .distinct.filterNot(_.isEmpty)

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => originalNumericalColumns2DArray.flatten.distinct ++ originalCategoricalColumns2DArray.flatten.distinct ++ scopeTextVariables2DArray.flatten.distinct

    }
    primaryKeyColumns ++ additionalColumns ++ singleValuesVariables ++ pageVariables
  }

  def getColumnNamesVariablesPublishPageLevel(pageNum: Int): Array[String] = {
    val currentPageAssembledColumns = new ArrayBuffer[String]()
    currentPageAssembledColumns ++= primaryKeyColumns ++ additionalColumns ++ singleValuesVariables

    ConfigUtils.variablesScope match {

      case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>

        currentPageAssembledColumns ++= originalNumericalColumns1DArray ++ originalCategoricalColumns1DArray ++ scopeTextVariables1DArray

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>

        if (categoricalColumns2DArray.nonEmpty && categoricalColumns2DArray.isDefinedAt(pageNum))
          currentPageAssembledColumns ++= categoricalColumns2DArray(pageNum)

        if (originalNumericalColumns2DArray.nonEmpty && originalNumericalColumns2DArray.isDefinedAt(pageNum))
          currentPageAssembledColumns ++= originalNumericalColumns2DArray(pageNum)

        if (scopeTextVariables2DArray.isDefinedAt(pageNum) && scopeTextVariables2DArray.isDefinedAt(pageNum))
          currentPageAssembledColumns ++= scopeTextVariables2DArray(pageNum)

    }

    currentPageAssembledColumns
      .filterNot(_.isEmpty)
      .distinct
      .toArray
  }

  lazy val randomVariable: String = if (!FlashMLConfig.getString(FlashMLConstants.RANDOM_VARIABLE).isEmpty)
    FlashMLConfig.getString(FlashMLConstants.RANDOM_VARIABLE)
  else if (FlashMLConfig.getString(FlashMLConstants.SAMPLING_TYPE) == FlashMLConstants.SAMPLING_TYPE_CONDITIONAL)
    FlashMLConstants.RANDOM_VARIABLE_COLUMN_NAME
  else ""

  lazy val plattScalingEnabled = FlashMLConfig.getBool(FlashMLConstants.SVM_PLATT_SCALING_ENABLED)

  lazy val mlAlgorithm = FlashMLConfig.getString(FlashMLConstants.ALGORITHM)

  // Platt Scaling is enabled if the User chooses SVM, and sets the Platt Scaling Option
  // in the config file to True
  lazy val isPlattScalingReqd = mlAlgorithm.equals(FlashMLConstants.SVM) && plattScalingEnabled

  lazy val topKIntentColumnName: String = FlashMLConfig
    .getString(FlashMLConstants.TOP_K_INTENTS_COLUMN_NAME)

  lazy val topKValue: Int = FlashMLConfig
    .getInt(FlashMLConstants.TOP_K_INTENTS_K_VALUE)

  /*
  lazy val intermediatePreprocessingVariables = {
    preprocessingVariablesScope match {
      case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE => None

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE => None

      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE => None

      // No need to support checking/ error handling of erroneous scope option

    }


  }*/

  object DataSetType extends Enumeration {
    val Train, Test, Validate = Value
  }

  type DataSet = DataSetType.Value

  //PREPROCESSING STEP - VECTORIZATION STEP CHANGES
  lazy val pagewisePreprocessingIntVariables: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer.fill(if (isPageLevelModel) numPages else 1)(ArrayBuffer[String]())

  /**
    * Number of spaces (indentation) used for formatting publish scripts.
    */
  lazy val defaultIndent: Int = if (modelingMethod.contains(FlashMLConstants.PAGE_LEVEL)) 2 else 0

  /**
    * Number of pages for page level models
    */
  lazy val numPageConfig: String = FlashMLConfig
    .getString(FlashMLConstants.EXPERIMENT_NUMBER_OF_PAGES)

  lazy val numPages: Int = numPageConfig.toInt

  lazy val isPageLevelModel: Boolean = modelingMethod
    .contains(FlashMLConstants.PAGE_LEVEL)

  lazy val toSavePoint: Boolean = FlashMLConfig.getBool(FlashMLConstants.SAVEPOINTING_REQUIRED)

  lazy val scopeProcessingErrorMessage = "Unidentified Scope option selected! Users can select between the options noPage, allPage or perPage (case insensitive)"

  val pos_prob: UserDefinedFunction = udf((a: DenseVector) => a(1))

  // Hard coded for optimal efficiency
  lazy val parallelism = 3

}