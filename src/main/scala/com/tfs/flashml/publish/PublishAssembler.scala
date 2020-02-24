package com.tfs.flashml.publish

import com.tfs.flashml.core.DirectoryCreator
import com.tfs.flashml.publish.featureengineering.VectorizationPublisher
import com.tfs.flashml.publish.model.ModelPublisher
import com.tfs.flashml.publish.featureGeneration.FeatureGenerationPublisher
import com.tfs.flashml.publish.preprocessing.PreprocessingPublisher
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.hadoop.fs.Path
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig, PublishUtils}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.io.Source

/**
 * Class to assemble individual part files to construct the complete file.
 */
object PublishAssembler
{
  private val log = LoggerFactory.getLogger(getClass)

  private val textVariableColumns= ConfigUtils.scopeTextVariables
  private val categoricalVariableColumns = ConfigUtils.scopeCategoricalVariables
  private val numericVariablesColumns = ConfigUtils.scopeNumericalVariables

  private val pageVariable = FlashMLConfig.getString(FlashMLConstants.PAGE_VARIABLE)
  val schemaFile: String = FlashMLConfig.getString(FlashMLConstants.SCHEMA_FILE)

  val ss: SparkSession = SparkSession.builder().getOrCreate()
  val mdrFqnPath = new Path(DirectoryCreator.getPublishPath(),"mdrFqnAndVersion")
  private val mdrFqnAndVersionSet = scala.collection.mutable.Set[String]()

  private val schema: mutable.Map[String, String] = Source.fromFile(schemaFile)
    .getLines
    .foldLeft(mutable.Map[String,String]())((accumulator,line) => {
      if (line.nonEmpty) {
        val varSchema = line.split("\t")
        val schemaSplit = varSchema(1).split("@")
        mdrFqnAndVersionSet += schemaSplit.mkString("\t")
        accumulator += varSchema(0) -> schemaSplit(0)
      }
      else {
        accumulator
      }
    })

  private val numOfPages: Int = if (ConfigUtils.isPageLevelModel)
  {
    FlashMLConfig.getInt(FlashMLConstants.EXPERIMENT_NUMBER_OF_PAGES)
  }
  else
    0

  def generateJS =
  {
    val mdrFqn = new StringBuilder
    mdrFqn ++= mdrFqnAndVersionSet.mkString("\n")

    DirectoryCreator.deleteDirectory(mdrFqnPath)
    log.info("Saving distinct MDR function and its version")

    ss.sparkContext
      .parallelize(Seq(mdrFqn),1)
      .saveAsTextFile(FlashMLConfig.getString(FlashMLConstants.NAME_NODE_URI) + "/" + mdrFqnPath.toString)

    val globalVar = mutable.Set[String]()

    if (ConfigUtils.isPageLevelModel)
    {
      val pageLevelJS = getPageLevelTopStructure
      var pagevarSetList = mutable.ArrayBuffer[mutable.Set[String]]()
      val globalVarSet = mutable.Set[String]()
      var pageLevelJSTmp = new StringBuilder

      for (pageNumber <- 1 to numOfPages)
      {
        pagevarSetList += getFeaturesSetFromViewJS(pageNumber - 1)
      }
      globalVarSet ++= pagevarSetList.reduce(_&_)
      pageLevelJS ++= globalVarSet.mkString("")
      for (pageNumber <- 1 to numOfPages)
      {
        pageLevelJSTmp ++= PublishUtils.getNewLine + PublishUtils.indent(1) + (pageNumber match
        {
          case 1 => "if (c_page_count == " + pageNumber + ") {"
          case `numOfPages` => "} else {"
          case _ => "} else if (c_page_count == " + pageNumber + ") {"
        })

        val featuresDefinitionJSSet:mutable.Set[String] = getFeaturesSetFromViewJS(pageNumber - 1)
        val featuresDefinitionJS = (featuresDefinitionJSSet &~ globalVarSet).mkString("")

        val preprocessingJS = PreprocessingPublisher.generateJS(pageNumber,globalVar)
        val featureGenerationJS = FeatureGenerationPublisher.generateJS(pageNumber, globalVar)
        val vectorizationJS = VectorizationPublisher.generateJS(pageNumber, globalVar)

        val modelJS = ModelPublisher.generateJS(pageNumber, globalVar)

        pageLevelJSTmp ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent) + "function " + ConfigUtils.mlAlgorithm + pageNumber + "(){"

        pageLevelJSTmp ++= featuresDefinitionJS ++ preprocessingJS ++ featureGenerationJS ++ vectorizationJS ++ modelJS

        pageLevelJSTmp ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent) + "}" + PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent) + "return " + ConfigUtils.mlAlgorithm + +pageNumber + "();"
      }

      pageLevelJSTmp ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "}"

      pageLevelJSTmp ++= PublishUtils.getNewLine + "}" + PublishUtils.getNewLine + "MainModel();"
      pageLevelJS ++= globalVar.mkString("")
      pageLevelJS ++= pageLevelJSTmp

      pageLevelJS
    }
    else {
      val featuresDefinitionJS = getFeaturesFromViewJS(0)
      val preprocessingJS = PreprocessingPublisher.generateJS(0, globalVar)

      val featureGenerationJS = if(!ConfigUtils.featureGenerationConfig.isEmpty) FeatureGenerationPublisher.generateJS(0, globalVar) else new mutable.StringBuilder()
      val vectorizationJS = VectorizationPublisher.generateJS(0, globalVar)
      val modelJS = ModelPublisher.generateJS(0, globalVar)

      val JS = new StringBuilder
      JS ++= "function " + ConfigUtils.mlAlgorithm + "(){"
      JS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "var result = {};"
      JS ++= featuresDefinitionJS
      JS ++= globalVar.mkString("")
      JS ++= preprocessingJS ++ featureGenerationJS ++ vectorizationJS ++ modelJS
      JS ++= PublishUtils.getNewLine + "}" + PublishUtils.getNewLine + ConfigUtils.mlAlgorithm + "();"
    }
  }

  //Retrieves the features from view using FQ paths and creates a SET
  def getFeaturesSetFromViewJS(index: Int): mutable.Set[String] =
  {
    val featuresJS = new StringBuilder
    var isPageVariableInFeatures = false
    val isSingleIntent = ConfigUtils
      .isSingleIntent
    val setOfVariables = mutable.Set[String]()

    ConfigUtils.variablesScope match{
      case FlashMLConstants.SCOPE_PARAMETER_NO_PAGE | FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE =>

        (ConfigUtils.scopeTextVariables1DArray ++ numericVariablesColumns.asInstanceOf[Array[String]] ++ categoricalVariableColumns.asInstanceOf[Array[String]])
          .foreach(i => {

            isPageVariableInFeatures = if (i.equals(pageVariable)) true
            else isPageVariableInFeatures

            setOfVariables += PublishUtils.getNewLine + PublishUtils.indent(1) + "var " + i + " = " + schema.getOrElse(i, null) + "." + i + ";"

          })


      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>

        if(ConfigUtils.scopeTextVariables2DArray.isDefinedAt(index) && ConfigUtils.scopeTextVariables2DArray(index).nonEmpty){
          textVariableColumns(index).asInstanceOf[Array[String]].filterNot(_.isEmpty).foreach(i => {
            isPageVariableInFeatures = if (i.equals(pageVariable)) true
            else isPageVariableInFeatures

            setOfVariables += PublishUtils.getNewLine + PublishUtils.indent(1) + "var " + i + " = " + schema.getOrElse(i,null) + "." + i + ";"
          })
        }

        if(categoricalVariableColumns.asInstanceOf[Array[Array[String]]].isDefinedAt(index) && categoricalVariableColumns(index).asInstanceOf[Array[String]].nonEmpty){
          categoricalVariableColumns(index).asInstanceOf[Array[String]].distinct.filterNot(_.isEmpty).foreach(i => {
            isPageVariableInFeatures = if (i.equals(pageVariable)) true
            else isPageVariableInFeatures

            setOfVariables += PublishUtils.getNewLine + PublishUtils.indent(1) + "var " + i + " = " + schema.getOrElse(i,null) + "." + i + ";"

          })
        }

        if(numericVariablesColumns.asInstanceOf[Array[Array[String]]].isDefinedAt(index) && numericVariablesColumns(index).asInstanceOf[Array[String]].nonEmpty){
          numericVariablesColumns(index).asInstanceOf[Array[String]].distinct.filterNot(_.isEmpty).foreach(i=>{
            isPageVariableInFeatures = if (i.equals(pageVariable)) true
            else isPageVariableInFeatures

            setOfVariables += PublishUtils.getNewLine + PublishUtils.indent(1) + "var " + i + " = " + schema.getOrElse(i,null) + "." + i + ";"

          })
        }

    }

    if (isSingleIntent && !isPageVariableInFeatures)
    {
      setOfVariables += PublishUtils.getNewLine + PublishUtils.indent(1) + "var " + pageVariable + " = " + schema.getOrElse(pageVariable, null) + "." + pageVariable + ";"
    }
    setOfVariables
  }

  //Retrieves the features from view using FQ paths
  def getFeaturesFromViewJS(index: Int): StringBuilder =
  {
    val featuresJS = new StringBuilder
    var isPageVariableInFeatures = false
    val isSingleIntent = ConfigUtils
      .isSingleIntent

    ConfigUtils.variablesScope match {
      case FlashMLConstants.SCOPE_PARAMETER_ALL_PAGE | FlashMLConstants.SCOPE_PARAMETER_NO_PAGE =>

        (ConfigUtils.scopeTextVariables1DArray ++ numericVariablesColumns.asInstanceOf[Array[String]] ++ categoricalVariableColumns.asInstanceOf[Array[String]])
          .filterNot(_.isEmpty)
          .distinct
          .foreach(i => {

            isPageVariableInFeatures = if (i.equals(pageVariable)) true
            else isPageVariableInFeatures

            featuresJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " + i + " = " + schema.getOrElse(i, null) + "." + i + ";"
          })

      case FlashMLConstants.SCOPE_PARAMETER_PER_PAGE =>

        if (ConfigUtils.scopeTextVariables2DArray.isDefinedAt(index) && ConfigUtils.scopeTextVariables2DArray(index).nonEmpty) {
          textVariableColumns(index)
            .asInstanceOf[Array[String]]
            .filterNot(_.isEmpty)
            .distinct
            .foreach(i => {
              isPageVariableInFeatures = if (i.equals(pageVariable)) true
              else isPageVariableInFeatures

              featuresJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " + i + " = " + schema.getOrElse(i, null) + "." + i + ";"
            })
        }

        if (categoricalVariableColumns.asInstanceOf[Array[Array[String]]].isDefinedAt(index) && categoricalVariableColumns(index).asInstanceOf[Array[String]].nonEmpty) {
          categoricalVariableColumns(index)
            .asInstanceOf[Array[String]]
            .filterNot(_.isEmpty)
            .distinct
            .foreach(i => {
              isPageVariableInFeatures = if (i.equals(pageVariable)) true
              else isPageVariableInFeatures

              featuresJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " + i + " = " + schema.getOrElse(i, null) + "." + i + ";"
            })
        }

        if (numericVariablesColumns.asInstanceOf[Array[Array[String]]].isDefinedAt(index) && numericVariablesColumns(index).asInstanceOf[Array[String]].nonEmpty) {
          numericVariablesColumns(index).asInstanceOf[Array[String]].filterNot(_.isEmpty).distinct.foreach(i => {
            isPageVariableInFeatures = if (i.equals(pageVariable)) true
            else isPageVariableInFeatures

            featuresJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " + i + " = " + schema.getOrElse(i, null) + "." + i + ";"

          })
        }
    }

    if (isSingleIntent && !isPageVariableInFeatures)
    {
      featuresJS ++= PublishUtils.getNewLine + PublishUtils.indent(ConfigUtils.defaultIndent + 1) + "var " + pageVariable + " = " + schema.getOrElse(pageVariable, null) + "." + pageVariable + ";"
    }
    featuresJS
  }



  def getPageLevelTopStructure: StringBuilder = {
    val JS = new StringBuilder
    JS ++= "function MainModel() {"

    // Add model identifier to JS for later trace-back
    def getPraasModelID: String = {
      /*val client = FlashMLConfig.get(FlashMLConstants.PRAAS_PROJECT_CLIENT)
      val user = FlashMLConfig.get(FlashMLConstants.PRAAS_PROJECT_USER)
      val version = FlashMLConfig.get(FlashMLConstants.PRAAS_PROJECT_VERSION)
      val experiment = FlashMLConfig.get(FlashMLConstants.PRAAS_PROJECT_EXPERIMENT)
      client + "_" + user + "_" + version + "_" + experiment*/
      FlashMLConfig.getString(FlashMLConstants.FLASHML_PROJECT_ID)
    }

    JS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "// POWERED BY FLASHML ! id:  " + getPraasModelID

    JS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "var c_page_count = " + schema.getOrElse(pageVariable, null) + "." + pageVariable + ";"

    JS ++= PublishUtils.getNewLine + PublishUtils.indent(1) + "var result = {};"

    JS
  }
}
