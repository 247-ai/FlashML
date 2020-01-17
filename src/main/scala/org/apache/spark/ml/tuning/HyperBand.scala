package org.apache.spark.ml.tuning

import java.util.{Locale, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Random
import scala.math.{ceil, pow}
import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasCollectSubModels, HasParallelism}
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils
import org.apache.spark.ml.linalg._
import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, LBFGS => BreezeLBFGS, LBFGSB => BreezeLBFGSB, OWLQN => BreezeOWLQN}
import breeze.numerics
import com.tfs.flashml.core.sampling.{RandomTrainTestSplitter, TrainTestSplitter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{OneVsRestCustom, OneVsRestCustomModel}
import org.apache.spark.ml.tuning.generators.{Generator, ParamGenerator}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
//import breeze.linalg._
//import breeze.math._
import breeze.numerics._

/**
  * Params for [[HyperBand]] and [[HyperBandModel]].
  */
private[ml] trait HyperBandParams extends ValidatorParams
{
    /**
      * Param for number of folds for cross validation.  Must be &gt;= 2.
      * Default: 3
      *
      * @group param
      * val numFolds: IntParam = new IntParam(this, "numFolds",
      * "number of folds for cross validation (>= 2)", ParamValidators.gtEq(2))
      * *
      * /** @group getParam */
      * def getNumFolds: Int = $(numFolds)
      * *
      * setDefault(numFolds -> 3)*/

    val estimatorParamRangeMaps: Param[Array[ParamMap]] =
        new Param(this, "estimatorParamRangeMaps", "param range maps for the estimator")

    val estimatorParamIterableMaps: Param[Array[ParamMap]] =
        new Param(this, "estimatorParamIterableMaps", "param iterable maps for the estimator")

    /**
      * Param for maximum number of hyperband iterations, i.e maximum hyperband iterations per configuration.
      * 1 hyperband iteration = iterationMultiplier spark iterations
      * Default: 81
      */
    val maxHyperbandIter: IntParam = new IntParam(this, "maxHyperbandIter", "maximum number of hyperband iterations")

    setDefault(maxHyperbandIter -> 81)


    /**
      * Number of spark iteration per hyperband iterations
      * * Default: 20
      */
    val iterationMultiplier = new IntParam(this, "iterationMultiplier", "specify no of spark iterations per hyperband iteration")
    setDefault(iterationMultiplier -> 20)
    /* defines downsampling rate (default=3) */

    /**
      * Specify the max iterations for the best model in each hyperband outer loop. i.e the max iterations for best
      * model in one set of
      * configurations.
      * Default: 1000
      **/
    val maxIterationsFinalModel = new IntParam(this, "maxIterationsFinalModel", "number of iterations for the best model in each set of configuration")
    setDefault(maxIterationsFinalModel, 1000)

    /**
      * Param for downsampling rate. Must be &gt; 0
      * Default: 3
      */
    val eta = new IntParam(this, "eta", "downsampling rate", ParamValidators.gt(0))
    setDefault(eta -> 3)

    /*
    * Proportion of data to use for training. It should be in the range 0 to 1 - (0,1). Both ends exclusive
    * Default: 0.8
    * */
    val trainSize = new DoubleParam(this, "trainSize", "train data portion in decimals",
        ParamValidators.inRange(0, 1, false, false))
    setDefault(trainSize -> 0.8)

}

/**
  * Hyperband is a hyperparameter optimization algorithm.
  * "Hyperband formulates hyperparameter optimization as a pure-exploration nonstochastic
  * infinite-armed bandit problem where a predefined resource like iterations, data
  * samples, or features is allocated to randomly sampled configurations."
  *
  * In this implementation the input data is split into train & test & a given performance metric is
  * evaluated on test set to select the best hyperparameter. At the end final model is created using the
  * best hyperparameter on the complete data set that is provided.
  *
  * Hyperband paper - https://arxiv.org/abs/1603.06560
  * Another blog by one of the authors - https://homes.cs.washington.edu/~jamieson/hyperband.html
  */
class HyperBand(override val uid: String)
        extends Estimator[HyperBandModel]
                with HyperBandParams with HasParallelism with HasCollectSubModels
                with MLWritable with Logging
{
    private var estimatorParamSpecifier = new ParamRangeSpecifier()

    var paramGenerator: Generator[ParamMap] = ParamGenerator.empty

    var trainTestSplitter:TrainTestSplitter = new RandomTrainTestSplitter($(trainSize),$(seed))


    def this() = this(Identifiable.randomUID("hyperband"))


    def logEta(x: Double) = numerics.log(x) / numerics.log($(eta))

    /* number of unique executions of Successive Halving (minus one) */
    def sMax = logEta($(maxHyperbandIter)).toInt

    /* total number of iterations (without reuse) per execution of Succesive Halving (n,r) */
    def B = (sMax + 1) * $(maxHyperbandIter)


    /** @group setParam */
    def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

    /** @group setParam */
    def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

    /* adding a default value with empty param Map. This variable is not used in hyperband, as it is generated
       on the fly*/
    setEstimatorParamMaps(new ParamGridBuilder().build())

    def setParamGenerator(value: Generator[ParamMap]): this.type =
    {
        this.paramGenerator = value
        this
    }

    /**
      * If trainTestSplitter is assigned then seed value & trainPercent set in code won't be used
      * @param value
      * @return
      */
    def setTrainTestSplitter(value:TrainTestSplitter):this.type = {
        this.trainTestSplitter = value
        this
    }


    /** @group setParam */
    def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

    def setMaxHyperbandIter(value: Int): this.type = set(maxHyperbandIter, value)

    def setIterationMultiplier(value: Int): this.type = set(iterationMultiplier, value)

    def setMaxIterationsFinalModel(value: Int): this.type = set(maxIterationsFinalModel, value)

    def setEta(value: Int): this.type = set(eta, value)

    def setTrainSize(value: Double): this.type = set(trainSize, value)

    def setTestSize(value: Double): this.type = set(trainSize, 1 - value)

    /** @group setParam */
    def setSeed(value: Long): this.type = set(seed, value)

    /**
      * Set the maximum level of parallelism to evaluate models in parallel.
      * Default is 1 for serial evaluation
      *
      * @group expertSetParam
      */
    def setParallelism(value: Int): this.type = set(parallelism, value)

    override def fit(dataset: Dataset[_]): HyperBandModel = instrumented
    {

        /**
          * This returns a list with no of successive halving rounds to perform, corresponding to each set of unique
          * configurations.
          * Starting with loop with more hyperband configurations, hence decreasing order of successive halving values
          **/
        def getMaxSuccessiveHalvingCount =
        { //the count of values in srange will be sMax+1
            sMax to 0 by -1
        }

        /**
          * Get the intial no of iterations that a configuration have to be run
          **/
        def getInitialIterationsCount(maxHyperbandIteration: Int, etaValue: Int, s: Int) =
        {
            maxHyperbandIteration * pow(etaValue, -s)
        }

        /**
          * Get the number of configurations to generate for the hyperband outerloop
          **/
        def getConfigCount(maxHyperbandIteration: Int, etaValue: Int, B: Int, s: Int) =
        {
            ceil((B / maxHyperbandIteration / (s + 1)).toInt * pow(etaValue, s)).toInt
        }

        /**
          * Reduced no of configurations during successing halving loop
          **/
        def reduceConfigurationsCount(etaValue: Int, n: Int, i: Int) =
        {
            n * pow(etaValue, -i)
        }

        /**
          * Increased no of iterations during successive halving loop
          **/
        def increaseIterationsCount(etaValue: Int, r: Double, i: Int) =
        {
            r * pow(etaValue, i)
        }

        /**
          * Decide the no of max iterations to set based on whether the current successive halving loop is the last one.
          * The original hyperband implementation doesn't have the concept of separate max iterations only for the
          * last model.
          **/
        def findMaxIterations(iterationMultiplier: Int, successiveHalvingIteration: Int, r_i: Double,
                              maxIterationFinal: Int) =
        {
            if (successiveHalvingIteration == 0)
                maxIterationFinal
            else (r_i * iterationMultiplier).toInt
        }

        def initializeBestMetric(eval: Evaluator) =
        {
            if (eval.isLargerBetter) 0.0
            else Double.MaxValue
        }

        instr =>
            val schema = dataset.schema
            transformSchema(schema, logging = true)
            val sparkSession = dataset.sparkSession
            val est = $(estimator)
            val eval = $(evaluator)
            val epm = $(estimatorParamMaps)
            val maxHyperbandIteration = $(maxHyperbandIter)
            val iterMultiplier = $(iterationMultiplier)
            val maxIterationFinal = $(maxIterationsFinalModel)
            val etaValue = $(eta)
            val trainPercent = $(trainSize)



            // Create execution context based on $(parallelism)
            val executionContext = getExecutionContext

            instr.logPipelineStage(this)
            instr.logDataset(dataset)
            instr.logParams(this, maxHyperbandIter, iterationMultiplier, eta, seed, parallelism)
            logTuningParams(instr)

            val collectSubModelsParam = $(collectSubModels)

            val inpDataset = dataset.asInstanceOf[Dataset[Row]]

            // Creating a train test split using random split
            //TODO make type of split configurable
            val trainTest = trainTestSplitter.split(inpDataset)
//            val trainTestSplit = Array(trainPercent, testPercent)
//            val trainTest = dataset.randomSplit(trainTestSplit, $(seed))

            val trainingDataset = trainTest(0).cache()
            val validationDataset = trainTest(1).cache()
            // Creating an array of param map given the range of values for each parameter
            // the count of values in srange will be sMax+1
            val maxSuccessiveHalvingRange = getMaxSuccessiveHalvingCount
            val B = (sMax + 1) * maxHyperbandIteration


            // This is the metric for the best entry,
            // this value would ideally keep on reducing with more successive halving loops
            // this is because the no of iterations increases with each successive halving loops
            var bestMetricOverall = initializeBestMetric(eval)
            var bestParamValueOverall: ParamMap = new ParamMap()

            val metricsBuffer = new ListBuffer[Double]()


            // Begin Finite Horizon Hyperband outlerloop. Repeat indefinetely.
            for (s <- maxSuccessiveHalvingRange)
            {
                // initial no of configurations
                val n = getConfigCount(maxHyperbandIteration, etaValue, B, s)
                // initial number of iterations to run configurations for
                val r = getInitialIterationsCount(maxHyperbandIteration, etaValue, s)
                // Begin Finite Horizon Successive Halving with (n,r)
                // we need entries = n, scala range function is inclusive of both ends
                var paramGridArray = (1 to n).map(_ => paramGenerator.getNext)

                // This is used to store the best metric in the present generated config
                // Eg: if we generated 81 configs, this is used to store best metric from
                //   these 81 configs till end of successive halving for these 81 configs
                var bestMetricCurrentConfigSet = initializeBestMetric(eval)
                var bestParamMapCurrentConfigSet = new ParamMap()
                //we need the it to be inclusive of value s in the range
                for (i <- 0 to s)
                {
                    // Run each of the nI configs for rI iterations and keep best nI/eta
                    val nI = reduceConfigurationsCount(etaValue, n, i)
                    // no of iterations to run for the current loop of successive halving
                    val rI = increaseIterationsCount(etaValue, r, i)

                    log.info("param grid values length " + paramGridArray.length + " values " + paramGridArray
                            .mkString(" , "))

                    trainingDataset.printSchema()

                    val valMetricsFutures = paramGridArray.zipWithIndex.map
                    { case (paramMap, paramIndex) =>
                        Future[Double]
                        {
                            //This won't work for estimators without maxIter,
                            // TODO will have to check if pipeline objects have maxIter in topLevel
                            val maxIterParam  = est match {
                                case oneVsRestCustom:OneVsRestCustom => oneVsRestCustom.getClassifier.getParam("maxIter")
                                case est => est.getParam("maxIter")
                            }
                            val maxIter: Int = findMaxIterations(iterMultiplier, i, rI, maxIterationFinal)
                            paramMap.put(maxIterParam, maxIter)
                            val model = est.fit(trainingDataset, paramMap).asInstanceOf[Model[_]]

                            val metric = eval.evaluate(model.transform(validationDataset))
                            metric
                        }(executionContext)
                    }

                    val valMetrics = valMetricsFutures.map(ThreadUtils.awaitResult(_, Duration.Inf))
                    //                log.info("validation metrics " + valMetrics.mkString(","))
                    // the number of configurations to retain for next round of successive halving
                    val noOfSetsToRetain = (nI / etaValue).toInt
                    log.info(s"No of sets to retain $noOfSetsToRetain \n value of n_i $nI no of iteration r_i $rI " +
                            s"value of n $n & value of r $r value of s $s paramGridArray length ${paramGridArray
                                    .length} is larger better ${eval.isLargerBetter}")
                    val zippedMetricValue = paramGridArray.zip(valMetrics).map
                    { case (p, m) => s"${p.toString()} =>  metric = $m" }.mkString("\n")
                    log.info(s"No of params in current loop ${paramGridArray.length} params & val metrics before " +
                            s"reducing - \n $zippedMetricValue")

                    val sortOrdering = if (eval.isLargerBetter) -1 else 1

                    val valMetricsSorted = valMetrics.zipWithIndex.sortBy(_._1 * sortOrdering)
                    val indexesToRetain = valMetricsSorted.map
                    { case (m, index) => index }.take(noOfSetsToRetain)
                    //                instr.logDebug("indexes to retain " + indexesToRetain.mkString(" , "))
                    val (topValueCurrentSuccessiveHalving, topIndex) = valMetricsSorted(0)
                    val topGridParamCurrentSuccessiveHalving = paramGridArray(topIndex)


                    bestMetricCurrentConfigSet = topValueCurrentSuccessiveHalving
                    bestParamMapCurrentConfigSet = topGridParamCurrentSuccessiveHalving

                    if (eval.isLargerBetter)
                    {
                        if (topValueCurrentSuccessiveHalving > bestMetricOverall)
                        {
                            bestParamValueOverall = topGridParamCurrentSuccessiveHalving
                            bestMetricOverall = topValueCurrentSuccessiveHalving
                        }
                    }
                    else
                    {
                        if (topValueCurrentSuccessiveHalving < bestMetricOverall)
                        {
                            bestParamValueOverall = topGridParamCurrentSuccessiveHalving
                            bestMetricOverall = topValueCurrentSuccessiveHalving
                        }
                    }

                    // Keeping top entries & removing entries performing worse from paramGridArray
                    paramGridArray = paramGridArray.zipWithIndex.filter
                    { case (v, index) => indexesToRetain.contains(index) }.map(_._1)
                    //                println("param grid values after reducing size length " + paramGridArray.length
                    // + " values " + paramGridArray.mkString(" , "))

                }
                metricsBuffer += bestMetricCurrentConfigSet
                log.info(s"Done with s = $s")
            }
            log.info("Best hyperparameter values " + bestParamValueOverall + " best metric " + bestMetricOverall)
            // Unpersist training & validation set once all metrics have been produced
            trainingDataset.unpersist()
            validationDataset.unpersist()

            // Using best parameters to train on the complete data provided to hyperband
            val bestModel = est.fit(dataset, bestParamValueOverall).asInstanceOf[Model[_]]

            copyValues(new HyperBandModel(uid, bestModel, metricsBuffer.toArray))
    }


    override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

    override def copy(extra: ParamMap): HyperBand =
    {
        val copied = defaultCopy(extra).asInstanceOf[HyperBand]
        if (copied.isDefined(estimator))
        {
            copied.setEstimator(copied.getEstimator.copy(extra))
        }
        if (copied.isDefined(evaluator))
        {
            copied.setEvaluator(copied.getEvaluator.copy(extra))
        }
        copied
    }

    // Currently, this only works if all [[Param]]s in [[estimatorParamMaps]] are simple types.
    // E.g., this may fail if a [[Param]] is an instance of an [[Estimator]].
    // However, this case should be unusual.
    override def write: MLWriter = new HyperBand.HyperBandWriter(this)
}

object HyperBand extends MLReadable[HyperBand]
{
    override def read: MLReader[HyperBand] = new HyperBandReader

    override def load(path: String): HyperBand = super.load(path)

    private[HyperBand] class HyperBandWriter(instance: HyperBand) extends MLWriter
    {

        ValidatorParams.validateParams(instance)

        override protected def saveImpl(path: String): Unit =
            ValidatorParams.saveImpl(path, instance, sc)
    }

    private class HyperBandReader extends MLReader[HyperBand]
    {

        /** Checked against metadata when loading model */
        private val className = classOf[HyperBand].getName

        override def load(path: String): HyperBand =
        {
            implicit val format = DefaultFormats

            val (metadata, estimator, evaluator, estimatorParamMaps) =
                ValidatorParams.loadImpl(path, sc, className)
            val cv = new HyperBand(metadata.uid)
                    .setEstimator(estimator)
                    .setEvaluator(evaluator)
                    .setEstimatorParamMaps(estimatorParamMaps)
            metadata.getAndSetParams(cv, skipParams = Option(List("estimatorParamMaps")))
            cv
        }
    }

}

/**
  * HyperBandModel contains the model with the highest average cross-validation
  * metric across folds and uses this model to transform input data. HyperBandModel
  * also tracks the metrics for each param map evaluated.
  *
  * @param bestModel  The best model selected from k-fold cross validation.
  * @param avgMetrics Average cross-validation metrics for each paramMap in
  *                   `HyperBand.estimatorParamMaps`, in the corresponding order.
  */
class HyperBandModel private[ml](
                                        @Since("1.4.0") override val uid: String,
                                        @Since("1.2.0") val bestModel: Model[_],
                                        @Since("1.5.0") val avgMetrics: Array[Double])
        extends Model[HyperBandModel] with HyperBandParams with MLWritable
{

    /** A Python-friendly auxiliary constructor. */
    private[ml] def this(uid: String, bestModel: Model[_], avgMetrics: JList[Double]) =
    {
        this(uid, bestModel, avgMetrics.asScala.toArray)
    }

    private var _subModels: Option[Array[Array[Model[_]]]] = None

    private[tuning] def setSubModels(subModels: Option[Array[Array[Model[_]]]])
    : HyperBandModel =
    {
        _subModels = subModels
        this
    }

    // A Python-friendly auxiliary method
    private[tuning] def setSubModels(subModels: JList[JList[Model[_]]])
    : HyperBandModel =
    {
        _subModels = if (subModels != null)
        {
            Some(subModels.asScala.toArray.map(_.asScala.toArray))
        }
        else
        {
            None
        }
        this
    }

    /**
      * @return submodels represented in two dimension array. The index of outer array is the
      *         fold index, and the index of inner array corresponds to the ordering of
      *         estimatorParamMaps
      * @throws IllegalArgumentException if subModels are not available. To retrieve subModels,
      *                                  make sure to set collectSubModels to true before fitting.
      */
    def subModels: Array[Array[Model[_]]] =
    {
        require(_subModels.isDefined, "subModels not available, To retrieve subModels, make sure " +
                "to set collectSubModels to true before fitting.")
        _subModels.get
    }

    def hasSubModels: Boolean = _subModels.isDefined

    override def transform(dataset: Dataset[_]): DataFrame =
    {
        transformSchema(dataset.schema, logging = true)
        bestModel.transform(dataset)
    }

    override def transformSchema(schema: StructType): StructType =
    {
        bestModel.transformSchema(schema)
    }

    override def copy(extra: ParamMap): HyperBandModel =
    {
        val copied = new HyperBandModel(
            uid,
            bestModel.copy(extra).asInstanceOf[Model[_]],
            avgMetrics.clone()
        ).setSubModels(HyperBandModel.copySubModels(_subModels))
        copyValues(copied, extra).setParent(parent)
    }

    override def write: HyperBandModel.HyperBandModelWriter =
    {
        new HyperBandModel.HyperBandModelWriter(this)
    }
}

object HyperBandModel extends MLReadable[HyperBandModel]
{

    private[HyperBandModel] def copySubModels(subModels: Option[Array[Array[Model[_]]]])
    : Option[Array[Array[Model[_]]]] =
    {
        subModels.map(_.map(_.map(_.copy(ParamMap.empty).asInstanceOf[Model[_]])))
    }

    override def read: MLReader[HyperBandModel] = new HyperBandModelReader

    override def load(path: String): HyperBandModel = super.load(path)

    /**
      * Writer for HyperBandModel.
      * HyperBandModelWriter is used to write the best model from the experiment
      *
      * @param instance HyperBandModel instance used to construct the writer
      */
    final class HyperBandModelWriter private[tuning](instance: HyperBandModel) extends MLWriter
    {

        ValidatorParams.validateParams(instance)

        override protected def saveImpl(path: String): Unit =
        {
            val persistSubModelsParam = optionMap.getOrElse("persistsubmodels",
                if (instance.hasSubModels) "true"
                else "false")

            require(Array("true", "false").contains(persistSubModelsParam.toLowerCase(Locale.ROOT)),
                s"persistSubModels option value ${persistSubModelsParam} is invalid, the possible " +
                        "values are \"true\" or \"false\"")
            val persistSubModels = persistSubModelsParam.toBoolean

            import org.json4s.JsonDSL._
            val extraMetadata = ("avgMetrics" -> instance.avgMetrics.toSeq) ~
                    ("persistSubModels" -> persistSubModels)
            ValidatorParams.saveImpl(path, instance, sc, Some(extraMetadata))
            val bestModelPath = new Path(path, "bestModel").toString
            instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)

            //commenting out as we are not saving sub models
            /*if (persistSubModels) {
                require(instance.hasSubModels, "When persisting tuning models, you can only set " +
                        "persistSubModels to true if the tuning was done with collectSubModels set to true. " +
                        "To save the sub-models, try rerunning fitting with collectSubModels set to true.")
                val subModelsPath = new Path(path, "subModels")
                for (splitIndex <- 0 until instance.getNumFolds) {
                    val splitPath = new Path(subModelsPath, s"fold${splitIndex.toString}")
                    for (paramIndex <- 0 until instance.getEstimatorParamMaps.length) {
                        val modelPath = new Path(splitPath, paramIndex.toString).toString
                        instance.subModels(splitIndex)(paramIndex).asInstanceOf[MLWritable].save(modelPath)
                    }
                }
            }*/
        }
    }

    private class HyperBandModelReader extends MLReader[HyperBandModel]
    {
        /** Checked against metadata when loading model */
        private val className = classOf[HyperBandModel].getName

        override def load(path: String): HyperBandModel =
        {
            implicit val format = DefaultFormats

            val (metadata, estimator, evaluator, estimatorParamMaps) =
                ValidatorParams.loadImpl(path, sc, className)
            //            val numFolds = (metadata.params \ "numFolds").extract[Int]
            val bestModelPath = new Path(path, "bestModel").toString
            val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
            val avgMetrics = (metadata.metadata \ "avgMetrics").extract[Seq[Double]].toArray
            val persistSubModels = (metadata.metadata \ "persistSubModels")
                    .extractOrElse[Boolean](false)

            //not saving sub models
            /*val subModels: Option[Array[Array[Model[_]]]] = if (persistSubModels) {
                val subModelsPath = new Path(path, "subModels")
                val _subModels = Array.fill(numFolds)(Array.fill[Model[_]](
                    estimatorParamMaps.length)(null))
                for (splitIndex <- 0 until numFolds) {
                    val splitPath = new Path(subModelsPath, s"fold${splitIndex.toString}")
                    for (paramIndex <- 0 until estimatorParamMaps.length) {
                        val modelPath = new Path(splitPath, paramIndex.toString).toString
                        _subModels(splitIndex)(paramIndex) =
                                DefaultParamsReader.loadParamsInstance(modelPath, sc)
                    }
                }
                Some(_subModels)
            } else None*/

            val model = new HyperBandModel(metadata.uid, bestModel, avgMetrics)
            //                    .setSubModels(subModels)
            model.set(model.estimator, estimator)
                    .set(model.evaluator, evaluator)
                    .set(model.estimatorParamMaps, estimatorParamMaps)
            metadata.getAndSetParams(model, skipParams = Option(List("estimatorParamMaps")))
            model
        }
    }

}

