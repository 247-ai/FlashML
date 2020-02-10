package com.tfs.flashml.core.modeltraining

import com.tfs.flashml.dal.SavePointManager
import com.tfs.flashml.util.conf.{ConfigValidatorException, FlashMLConstants}
import com.tfs.flashml.util.{ConfigUtils, FlashMLConfig}
import org.apache.spark.SparkException
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.tuning.{ParamGridBuilder, ParamRangeSpecifier}
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

/**
  * This object contains the methods for getting estimators, paramGrids and Hyperbands for each of FlashML's supported ML algorithms
  */
object ModelTrainingUtils
{
    val log: Logger = LoggerFactory.getLogger(getClass)

    lazy val isHyperParam = FlashMLConfig.hasKey(FlashMLConstants.HYPER_PARAM_OP) && FlashMLConfig.getBool(FlashMLConstants.HYPER_PARAM_OP)
    private val algorithm: String = ConfigUtils.mlAlgorithm

    // Question asking for better solution of the same, unanswered on StackOverflow since April 2017. URL: https://stackoverflow.com/questions/42984702/how-to-get-feature-vector-column-length-in-spark-pipeline
    // When using ML Algorithm MultiLayerPerceptron, inputFeaturesSize value are to be computed internally and not accepted from the user
    // It is the first value in the MLP array Param 'Layers'
    // It is computed from a sample row of the vectorized training DF
    lazy val inputFeaturesSizeMLP = SavePointManager
            .loadData(FlashMLConstants.VECTORIZATION)
            .apply(0)
            .take(1)(0)
            .getAs("features")
            .asInstanceOf[SparseVector]
            .size

    // Number of unique labels/ classes in data
    // Computed directly from the input data
    // All rows of the responseVariable column read from the input data and counted
    lazy val finalclassesMLP = SavePointManager
            .loadInputData
            .select(FlashMLConfig.getString(FlashMLConstants.RESPONSE_VARIABLE))
            .distinct()
            .collect()
            .length

    lazy val paramGridLayersMLP: Array[Array[Int]] = {
        FlashMLConfig
                .get2DArrayInt(FlashMLConstants.MLP_INTERMEDIATE_LAYERS)
                .map(arr => {
                    Array(inputFeaturesSizeMLP) ++ arr ++ Array(finalclassesMLP)
                })
    }

    def convertToDoubleList[T, U](list: Iterable[_]): Iterable[Double] =
    {
        list.map(castToDouble)
    }

    def castToDouble[T](value: T): Double =
    {
        value match
        {
            case v: Int =>
                v.toDouble
            // value was coming in as java.lang.Integer,
            // but if scala Int is kept before java int this case was not required
            /*case v:Integer => println("java integer to double casting")
                v.toDouble*/
            case v: Double =>
                v
            case _ =>
                throw new Exception(s"invalid type for casting to double. Type is ${value.getClass}")
        }
    }

    /**
      * In FlashML, ParamGrid and CrossValidation are tied together
      * Enabling CV requires user to enter multiple values for each parameter used in the ML algorithm
      * @param estimator used to fetch all params related to the ML Algorithm
      * @return Array of each combination of params entered by user in array
      * The set of each combinations is used for each fold in the k-fold CV
      */
    def getParamGridFlashML(estimator: Estimator[_]): Array[ParamMap] =
    {
        val estimatorCanonicalName = estimator
                .getClass
                .getCanonicalName

        val algoName = estimatorCanonicalName
                .substring(estimatorCanonicalName.lastIndexOf(".") + 1)

        algoName match
        {
            case "LogisticRegression" => new ParamGridBuilder()
                    .addGrid(estimator.asInstanceOf[LogisticRegression].regParam, FlashMLConfig.getDoubleArray
                    (FlashMLConstants.LR_REGULARISATION))
                    .addGrid(estimator.asInstanceOf[LogisticRegression].maxIter, FlashMLConfig.getIntArray
                    (FlashMLConstants.LR_ITERATIONS))
                    .addGrid(estimator.asInstanceOf[LogisticRegression].elasticNetParam, FlashMLConfig.getDoubleArray
                    (FlashMLConstants.LR_ELASTIC_NET))
                    .addGrid(estimator.asInstanceOf[LogisticRegression].standardization, FlashMLConfig.getBoolArray
                    (FlashMLConstants.LR_STANDARDIZATION))
                    .build()

            case "LinearSVC" => new ParamGridBuilder()
                    .addGrid(estimator.asInstanceOf[LinearSVC].regParam, FlashMLConfig.getDoubleArray
                    (FlashMLConstants.SVM_REGULARISATION))
                    .addGrid(estimator.asInstanceOf[LinearSVC].maxIter, FlashMLConfig.getIntArray(FlashMLConstants
                            .SVM_ITERATIONS))
                    .addGrid(estimator.asInstanceOf[LinearSVC].standardization, FlashMLConfig.getBoolArray
                    (FlashMLConstants.SVM_STANDARDIZATION))
                    .build()

            case "OneVsRestCustom" => if (algoName.equals(FlashMLConstants.LOGISTIC_REGRESSION))
                new ParamGridBuilder()
                        .addGrid(estimator.asInstanceOf[OneVsRestCustom].getClassifier
                                .asInstanceOf[LogisticRegression].regParam, FlashMLConfig.getDoubleArray
                        (FlashMLConstants.LR_REGULARISATION))
                        .addGrid(estimator.asInstanceOf[OneVsRestCustom].getClassifier
                                .asInstanceOf[LogisticRegression].maxIter, FlashMLConfig.getIntArray(FlashMLConstants
                                .LR_ITERATIONS))
                        .addGrid(estimator.asInstanceOf[OneVsRestCustom].getClassifier
                                .asInstanceOf[LogisticRegression].elasticNetParam, FlashMLConfig.getDoubleArray
                        (FlashMLConstants.LR_ELASTIC_NET))
                        .addGrid(estimator.asInstanceOf[OneVsRestCustom].getClassifier
                                .asInstanceOf[LogisticRegression].standardization, FlashMLConfig.getBoolArray
                        (FlashMLConstants.LR_STANDARDIZATION))
                        .build()
            else if (algoName.equals(FlashMLConstants.SVM))
                new ParamGridBuilder()
                        .addGrid(estimator.asInstanceOf[OneVsRestCustom].getClassifier.asInstanceOf[LinearSVC]
                                .regParam, FlashMLConfig.getDoubleArray(FlashMLConstants.SVM_REGULARISATION))
                        .addGrid(estimator.asInstanceOf[OneVsRestCustom].getClassifier.asInstanceOf[LinearSVC]
                                .maxIter, FlashMLConfig.getIntArray(FlashMLConstants.SVM_ITERATIONS))
                        .addGrid(estimator.asInstanceOf[OneVsRestCustom].getClassifier.asInstanceOf[LinearSVC]
                                .standardization, FlashMLConfig.getBoolArray(FlashMLConstants.SVM_STANDARDIZATION))
                        .build()
            else
                new ParamGridBuilder().build()
            case FlashMLConstants.NAIVE_BAYES => new ParamGridBuilder()
                    .build()

            case "RandomForestClassifier" =>
                new ParamGridBuilder()
                        .addGrid(estimator.asInstanceOf[RandomForestClassifier].maxDepth, FlashMLConfig.getIntArray
                        (FlashMLConstants.RF_MAXDEPTH))
                        .addGrid(estimator.asInstanceOf[RandomForestClassifier].impurity, FlashMLConfig
                                .getStringArray(FlashMLConstants.RF_IMPURITY))
                        .addGrid(estimator.asInstanceOf[RandomForestClassifier].numTrees, FlashMLConfig.getIntArray
                        (FlashMLConstants.RF_NUMBER_OF_TREES))
                        .addGrid(estimator.asInstanceOf[RandomForestClassifier].featureSubsetStrategy, FlashMLConfig
                                .getStringArray(FlashMLConstants.RF_FEATURESUBSETSTRATEGY))
                        .build()

            case "DecisionTreeClassifier" =>
                new ParamGridBuilder()
                        .addGrid(estimator.asInstanceOf[DecisionTreeClassifier].maxDepth, FlashMLConfig.getIntArray
                        (FlashMLConstants.DT_MAX_DEPTH))
                        .addGrid(estimator.asInstanceOf[DecisionTreeClassifier].impurity, FlashMLConfig
                                .getStringArray(FlashMLConstants.DT_IMPURITY))
                        .addGrid(estimator.asInstanceOf[DecisionTreeClassifier].maxBins, FlashMLConfig.getIntArray
                        (FlashMLConstants.DT_MAX_BINS))
                        .addGrid(estimator.asInstanceOf[DecisionTreeClassifier].cacheNodeIds, FlashMLConfig
                                .getBoolArray(FlashMLConstants.DT_CACHE_NODE_ID_BOOL))
                        .build()

            case "GBTClassifier" =>
                new ParamGridBuilder()
                        .addGrid(estimator.asInstanceOf[GBTClassifier].impurity, FlashMLConfig.getStringArray
                        (FlashMLConstants.GBT_IMPURITY))
                        .addGrid(estimator.asInstanceOf[GBTClassifier].maxDepth, FlashMLConfig.getIntArray
                        (FlashMLConstants.GBT_MAX_DEPTH))
                        .addGrid(estimator.asInstanceOf[GBTClassifier].featureSubsetStrategy, FlashMLConfig
                                .getStringArray(FlashMLConstants.GBT_FEATURESUBSETSTRATEGY))
                        .addGrid(estimator.asInstanceOf[GBTClassifier].maxIter, FlashMLConfig.getIntArray
                        (FlashMLConstants.GBT_MAX_ITER))
                        .build()

            case "MultilayerPerceptronClassifier" =>
                new ParamGridBuilder()
                        .addGrid(estimator.asInstanceOf[MultilayerPerceptronClassifier].maxIter, FlashMLConfig
                                .getIntArray(FlashMLConstants.MLP_MAX_ITERATIONS))
                        .addGrid(estimator.asInstanceOf[MultilayerPerceptronClassifier].blockSize, FlashMLConfig
                                .getIntArray(FlashMLConstants.MLP_BLOCK_SIZE))
                        .addGrid(estimator.asInstanceOf[MultilayerPerceptronClassifier].layers, paramGridLayersMLP)
                        .build()

            case "NaiveBayes" =>
                new ParamGridBuilder()
                        .addGrid(estimator.asInstanceOf[NaiveBayes].smoothing, FlashMLConfig.getDoubleArray
                        (FlashMLConstants.NB_SMOOTHING))
                        .addGrid(estimator.asInstanceOf[NaiveBayes].modelType, FlashMLConfig.getStringArray
                        (FlashMLConstants.NB_MODEL_TYPE))
                        .build()
        }
    }

    def throwException(msg: String) =
    {
        log.error(msg)
        throw new SparkException(msg)
    }

    private val seedValue: Int = 999

    def getEstimator: Estimator[_] =
    {
        val isOvr = FlashMLConfig.getString(FlashMLConstants.BUILD_TYPE) == FlashMLConstants.BUILD_TYPE_OVR
        val crossValidationFolds = FlashMLConfig.getInt(FlashMLConstants.CROSS_VALIDATION)
        val doCrossValidate = crossValidationFolds > 1
        // if it's a cross valdation (grid search) or isHyperParam (if it is hyperparameter optimization using
        // hyperband)
        // For both of these cases the params are set from within the experiment
        val isParamGridUsed = doCrossValidate || isHyperParam

        def getOvr(classifier: Classifier[_, _, _]): Estimator[_] =
        {
            new OneVsRestCustom()
                    .setParallelism(ConfigUtils.parallelism)
                    .setClassifier(classifier)
                    .setLabelCol(ConfigUtils.getIndexedResponseColumn)
        }

        ConfigUtils.mlAlgorithm match
        {
            case FlashMLConstants.LOGISTIC_REGRESSION =>
                val lr = if (!isParamGridUsed)
                {
                    new LogisticRegression()
                            .setTol(1E-6)
                            .setFitIntercept(true)
                            .setRegParam(FlashMLConfig.getDouble(FlashMLConstants.LR_REGULARISATION))
                            .setMaxIter(FlashMLConfig.getInt(FlashMLConstants.LR_ITERATIONS))
                            .setElasticNetParam(FlashMLConfig.getDouble(FlashMLConstants.LR_ELASTIC_NET))
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                            .setStandardization(FlashMLConfig.getBool(FlashMLConstants.LR_STANDARDIZATION))
                }
                else
                {
                    new LogisticRegression()
                            .setTol(1E-6)
                            .setFitIntercept(true)
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                }

                if (isOvr)
                {
                    log.info(s"Model Training: Estimator is OVR with LR classifier")
                    getOvr(lr)
                }
                else
                {
                    log.info(s"Model Training: Estimator is LR")
                    lr
                }

            case FlashMLConstants.SVM =>
                val lsvc = if (!isParamGridUsed)
                {
                    new LinearSVC()
                            .setTol(1E-6)
                            .setFitIntercept(true)
                            .setRegParam(FlashMLConfig.getDouble(FlashMLConstants.SVM_REGULARISATION))
                            .setMaxIter(FlashMLConfig.getInt(FlashMLConstants.SVM_ITERATIONS))
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                            .setStandardization(FlashMLConfig.getBool(FlashMLConstants.SVM_STANDARDIZATION))
                }
                else
                {
                    new LinearSVC()
                            .setTol(1E-6)
                            .setFitIntercept(true)
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                }

                // SVC only supports binary classification. Hence for multi-class
                // models OVR is used by default
                if (isOvr)
                {
                    log.info(s"Model Training: Estimator is OVR with SVM")
                    getOvr(lsvc)
                }
                else
                {
                    log.info(s"Model Training: Estimator is SVM (LinearSVC)")
                    lsvc
                }

            case FlashMLConstants.RANDOM_FOREST =>

                val rf = if (!isParamGridUsed)
                {
                    new RandomForestClassifier()
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                            .setNumTrees(FlashMLConfig.getInt(FlashMLConstants.RF_NUMBER_OF_TREES))
                            .setSeed(seedValue)
                            .setImpurity(FlashMLConfig.getString(FlashMLConstants.RF_IMPURITY))
                            .setMaxDepth(FlashMLConfig.getInt(FlashMLConstants.RF_MAXDEPTH))
                            .setFeatureSubsetStrategy(FlashMLConfig.getString(FlashMLConstants
                                    .RF_FEATURESUBSETSTRATEGY))
                            .setProbabilityCol("probability")
                            .setRawPredictionCol("rawPrediction")
                }
                else
                {
                    new RandomForestClassifier()
                            .setSeed(seedValue)
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                            .setProbabilityCol("probability")
                            .setRawPredictionCol("rawPrediction")
                }

                    log.info(s"Model Training: Estimator is Random Forest")
                    rf

            case FlashMLConstants.GRADIENT_BOOSTED_TREES =>

                val gbt = if (!isParamGridUsed)
                {
                    new GBTClassifier()
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                            .setMaxIter(FlashMLConfig.getInt(FlashMLConstants.GBT_MAX_ITER))
                            .setFeatureSubsetStrategy(FlashMLConfig.getString(FlashMLConstants.GBT_FEATURESUBSETSTRATEGY))
                            .setSeed(seedValue)
                            .setMaxDepth(FlashMLConfig.getInt(FlashMLConstants.GBT_MAX_DEPTH))
                            .setImpurity(FlashMLConfig.getString(FlashMLConstants.GBT_IMPURITY))
                            .setProbabilityCol("probability")
                            .setRawPredictionCol("rawPrediction")
                }
                else
                {
                    new GBTClassifier()
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                            .setSeed(seedValue)
                            .setProbabilityCol("probability")
                            .setRawPredictionCol("rawPrediction")
                }

                    log.info(s"Model Training: Estimator is GBT")
                    gbt

            case FlashMLConstants.DECISION_TREES =>

                val dt = if (!isParamGridUsed)
                {
                    new DecisionTreeClassifier()
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                            .setImpurity(FlashMLConfig.getString(FlashMLConstants.DT_IMPURITY))
                            .setSeed(seedValue)
                            .setMaxDepth(FlashMLConfig.getInt(FlashMLConstants.DT_MAX_DEPTH))
                            .setCacheNodeIds(FlashMLConfig.getBool(FlashMLConstants.DT_CACHE_NODE_ID_BOOL))
                            .setMaxBins(FlashMLConfig.getInt(FlashMLConstants.DT_MAX_BINS))
                            .setProbabilityCol("probability")
                            .setRawPredictionCol("rawPrediction")
                }
                else
                {
                    new DecisionTreeClassifier()
                            .setSeed(seedValue)
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                            .setProbabilityCol("probability")
                            .setRawPredictionCol("rawPrediction")
                }

                    log.info(s"Model Training: Estimator is Decision Trees")
                    dt

            case FlashMLConstants.MULTILAYER_PERCEPTRON =>
                val mlp = if (!isParamGridUsed)
                {
                    new MultilayerPerceptronClassifier()
                            .setSeed(seedValue)
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                            .setBlockSize(FlashMLConfig.getInt(FlashMLConstants.MLP_BLOCK_SIZE))
                            .setMaxIter(FlashMLConfig.getInt(FlashMLConstants.MLP_MAX_ITERATIONS))
                            .setLayers(Array(inputFeaturesSizeMLP)++FlashMLConfig.getIntArray(FlashMLConstants.MLP_INTERMEDIATE_LAYERS)++Array(finalclassesMLP))
                            .setProbabilityCol("probability")
                            .setRawPredictionCol("rawPrediction")
                }
                else
                {
                    new MultilayerPerceptronClassifier()
                            .setSeed(seedValue)
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                            .setProbabilityCol("probability")
                            .setRawPredictionCol("rawPrediction")
                }

                mlp


            case FlashMLConstants.NAIVE_BAYES =>

                val nb = if(!isParamGridUsed){
                    new NaiveBayes()
                        .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                        .setProbabilityCol("probability")
                        .setRawPredictionCol("rawPrediction")
                        .setSmoothing(FlashMLConfig.getInt(FlashMLConstants.NB_SMOOTHING))
                        .setModelType(FlashMLConfig.getString(FlashMLConstants.NB_MODEL_TYPE)) }
                else{
                    new NaiveBayes()
                            .setLabelCol(ConfigUtils.getIndexedResponseColumn)
                            .setProbabilityCol("probability")
                            .setRawPredictionCol("rawPrediction")
                }

                    log.info(s"Model Training: Estimator is Naive Bayes")
                    nb

            case _ =>
            {
                throwException("Only Logistic Regression, Support Vector Machines (SVM), Random Forest, Naive Bayes, " +
                        "Gradient Boosted Trees algorithms are currently supported in FlashML")
            }
        }

    }

    private def assignIterableValue[T, U](param: Param[T], value: Iterable[U],
                                          paramRangeSpecifier: ParamRangeSpecifier): ParamRangeSpecifier =
    {
        paramRangeSpecifier.addGrid(param, value.asInstanceOf[Iterable[T]])
        return paramRangeSpecifier
    }

    private def updateParamRangeSpecifier[T, S](param: Param[T], configParam: S,
                                                paramRangeSpecifier: ParamRangeSpecifier)(implicit
                                                                                          paramTag: TypeTag[T],
                                                                                          valueTag: TypeTag[S])
    : ParamRangeSpecifier =
    {

        log.debug(s"type param tag is ${typeTag[T].tpe}")
        log.debug(s"type value tag is ${valueTag.tpe} & value is $configParam")


        configParam match
        {
            /* The type values mentioned for the HashMap[String,T] doesn't matter
            as it's lost due to type erasure, we are checking the type of Param separately &
            converting the value to double */
            case a: java.util.HashMap[String, T]@unchecked if (typeOf(paramTag) =:= typeOf[Double]) =>

                log.debug(s"hashmap with paramTag as double. Type class is ${a.getClass} typetag ${valueTag.tpe}")

                paramRangeSpecifier
                        .addGrid(param.asInstanceOf[Param[Double]], castToDouble(a.get("min")), castToDouble(a.get
                        ("max")))
            case a: java.util.HashMap[String, T]@unchecked => throw new ConfigValidatorException(s"invalid " +
                    s"configuration values $a")

            /*case l:List[T] => println(s"list as param class is ${l.getClass}")
                paramRangeSpecifier
                        .addGrid(param,l)*/

            //type check on valueTag doesn't work because the object is shown as type - java.lang.Object
            //            case l:java.util.ArrayList[T] if typeOf(valueTag) =:= typeOf[java.util.ArrayList[Double]] =>
            /* In a case where we are expecting double arraylist,
               if there is an integer value like 1,even if we specify the value as 1.0 in the configuration file it
               was picked up as an integer.
               We have to convert that integer to double & cast the entire list to a double list.*/
            case l: java.util.ArrayList[Double]@unchecked if (typeOf(paramTag) =:= typeOf[Double]) =>
                val convertedList = convertToDoubleList(asScalaBufferConverter(l).asScala)
                paramRangeSpecifier
                        .addGrid(param.asInstanceOf[Param[Double]], convertedList.asInstanceOf[Iterable[Double]])

            case l: java.util.ArrayList[T]@unchecked =>
                //this is for every list case except double.
                assignIterableValue(param, asScalaBufferConverter(l).asScala, paramRangeSpecifier)

            // TODO have to check for the correct type
            case d: T =>
                //                log.debug(s"single value of type ${d.getClass()} paramTag class ${paramTag.tpe} ")
                paramRangeSpecifier.addGrid(param, List(d))
            //TODO this condition doesn't even occur
            case d => throw new ConfigValidatorException(s"config param doesn't support an object of type ${
                d
                        .getClass
            }")
        }
        paramRangeSpecifier
    }

    def getHyperBandParamRange(estimator: Estimator[_], maxIter: Int, algoName: String) =
    {
        var maxIterations = maxIter

        val paramRangeSpec = algoName match
        {
            case "GBTClassifier" =>
                var paramRangeSpecifier = new ParamRangeSpecifier()

                maxIterations = FlashMLConfig.getInt(FlashMLConstants.GBT_MAX_ITER)

                updateParamRangeSpecifier(
                    estimator.asInstanceOf[GBTClassifier].maxDepth,
                    FlashMLConfig.config.getAnyRef(FlashMLConstants.GBT_MAX_DEPTH),
                    paramRangeSpecifier
                )

                updateParamRangeSpecifier(
                    estimator.asInstanceOf[GBTClassifier].maxIter,
                    FlashMLConfig.config.getAnyRef(FlashMLConstants.GBT_MAX_ITER),
                    paramRangeSpecifier
                )

                updateParamRangeSpecifier(
                    estimator.asInstanceOf[GBTClassifier].impurity,
                    FlashMLConfig.config.getAnyRef(FlashMLConstants.GBT_IMPURITY),
                    paramRangeSpecifier
                )

                updateParamRangeSpecifier(
                    estimator.asInstanceOf[GBTClassifier].featureSubsetStrategy,
                    FlashMLConfig.config.getAnyRef(FlashMLConstants.GBT_FEATURESUBSETSTRATEGY),
                    paramRangeSpecifier
                )

            case "LogisticRegression" =>
                maxIterations = FlashMLConfig.getInt(FlashMLConstants.LR_ITERATIONS)

                var paramRangeSpecifier = new ParamRangeSpecifier()

                updateParamRangeSpecifier(
                    estimator.asInstanceOf[LogisticRegression].regParam,
                    FlashMLConfig.config.getAnyRef(FlashMLConstants.LR_REGULARISATION),
                    paramRangeSpecifier
                )

                updateParamRangeSpecifier(
                    estimator.asInstanceOf[LogisticRegression].elasticNetParam,
                    FlashMLConfig.config.getAnyRef(FlashMLConstants.LR_ELASTIC_NET),
                    paramRangeSpecifier
                )

                updateParamRangeSpecifier(
                    estimator.asInstanceOf[LogisticRegression].standardization,
                    FlashMLConfig.config.getAnyRef(FlashMLConstants.LR_STANDARDIZATION),
                    paramRangeSpecifier
                )

            case "LinearSVC" =>
                maxIterations = FlashMLConfig.getInt(FlashMLConstants.SVM_ITERATIONS)
                var paramRangeSpecifier =
                    new ParamRangeSpecifier()

                updateParamRangeSpecifier(
                    estimator.asInstanceOf[LinearSVC].regParam,
                    FlashMLConfig.config.getAnyRef(FlashMLConstants.SVM_REGULARISATION),
                    paramRangeSpecifier)

                updateParamRangeSpecifier(
                    estimator.asInstanceOf[LinearSVC].standardization,
                    FlashMLConfig.config.getAnyRef(FlashMLConstants.SVM_STANDARDIZATION),
                    paramRangeSpecifier)


            case "OneVsRestCustom" =>
                var paramRangeSpecifier = new ParamRangeSpecifier()
                if (algorithm.equals(FlashMLConstants.LOGISTIC_REGRESSION))
                {
                    maxIterations = FlashMLConfig.getInt(FlashMLConstants.LR_ITERATIONS)
                    updateParamRangeSpecifier(estimator.asInstanceOf[OneVsRestCustom].getClassifier
                            .asInstanceOf[LogisticRegression].regParam, FlashMLConfig.config.getAnyRef
                    (FlashMLConstants.LR_REGULARISATION), paramRangeSpecifier)
                    updateParamRangeSpecifier(estimator.asInstanceOf[OneVsRestCustom].getClassifier
                            .asInstanceOf[LogisticRegression].elasticNetParam, FlashMLConfig.config.getAnyRef
                    (FlashMLConstants.LR_ELASTIC_NET), paramRangeSpecifier)
                    updateParamRangeSpecifier(estimator.asInstanceOf[OneVsRestCustom].getClassifier
                            .asInstanceOf[LogisticRegression].standardization, FlashMLConfig.config.getAnyRef
                    (FlashMLConstants.LR_STANDARDIZATION), paramRangeSpecifier)
                }

                else if (algorithm.equals(FlashMLConstants.SVM))
                {
                    maxIterations = FlashMLConfig.getInt(FlashMLConstants.SVM_ITERATIONS)

                    updateParamRangeSpecifier(
                        estimator.asInstanceOf[OneVsRestCustom].getClassifier.asInstanceOf[LinearSVC].regParam,
                        FlashMLConfig.config.getAnyRef(FlashMLConstants.SVM_REGULARISATION),
                        paramRangeSpecifier)

                    updateParamRangeSpecifier(estimator.asInstanceOf[OneVsRestCustom].getClassifier
                            .asInstanceOf[LinearSVC].standardization, FlashMLConfig.config.getAnyRef(FlashMLConstants
                            .SVM_STANDARDIZATION), paramRangeSpecifier)

                }
                paramRangeSpecifier


            case FlashMLConstants.NAIVE_BAYES =>
                var paramRangeSpecifier = new ParamRangeSpecifier()

                updateParamRangeSpecifier(
                    estimator.asInstanceOf[NaiveBayes].smoothing,
                    FlashMLConfig.config.getAnyRef(FlashMLConstants.NB_SMOOTHING),
                    paramRangeSpecifier
                )

                updateParamRangeSpecifier(
                    estimator.asInstanceOf[NaiveBayes].modelType,
                    FlashMLConfig.config.getAnyRef(FlashMLConstants.NB_MODEL_TYPE),
                    paramRangeSpecifier
                )

                paramRangeSpecifier
        }
        (maxIterations, paramRangeSpec)

    }

}
