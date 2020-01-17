package org.apache.spark.ml.classification

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, LBFGS => BreezeLBFGS, LBFGSB => BreezeLBFGSB, OWLQN => BreezeOWLQN}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.optim.aggregator.BinaryLogisticWithDoubleResponseAggregator
import org.apache.spark.ml.optim.loss.{L2Regularization, RDDLossFunction}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.VersionUtils

/**
  * Params for logistic regression.
  */
private[classification] trait BinaryLogisticRegressionWithDoubleResponseParams extends ProbabilisticClassifierParams
        with HasRegParam with HasElasticNetParam with HasMaxIter with HasFitIntercept with HasTol
        with HasStandardization with HasWeightCol with HasThreshold with HasAggregationDepth
{

    /**
      * Set threshold in binary classification, in range [0, 1].
      *
      * If the estimated probability of class label 1 is greater than threshold, then predict 1,
      * else 0. A high threshold encourages the model to predict 0 more often;
      * a low threshold encourages the model to predict 1 more often.
      *
      * Note: Calling this with threshold p is equivalent to calling `setThresholds(Array(1-p, p))`.
      *       When `setThreshold()` is called, any user-set value for `thresholds` will be cleared.
      *       If both `threshold` and `thresholds` are set in a ParamMap, then they must be
      *       equivalent.
      *
      * Default is 0.5.
      *
      * @group setParam
      */
    // TODO: Implement SPARK-11543?
    def setThreshold(value: Double): this.type = {
        if (isSet(thresholds)) clear(thresholds)
        set(threshold, value)
    }

    /**
      * Get threshold for binary classification.
      *
      * If `thresholds` is set with length 2 (i.e., binary classification),
      * this returns the equivalent threshold: {{{1 / (1 + thresholds(0) / thresholds(1))}}}.
      * Otherwise, returns `threshold` if set, or its default value if unset.
      *
      * @group getParam
      * @throws IllegalArgumentException if `thresholds` is set to an array of length other than 2.
      */
    override def getThreshold: Double = {
        checkThresholdConsistency()
        if (isSet(thresholds)) {
            val ts = $(thresholds)
            require(ts.length == 2, "Logistic Regression getThreshold only applies to" +
                    " binary classification, but thresholds has length != 2.  thresholds: " + ts.mkString(","))
            1.0 / (1.0 + ts(0) / ts(1))
        } else {
            $(threshold)
        }
    }

    /**
      * If `threshold` and `thresholds` are both set, ensures they are consistent.
      *
      * @throws IllegalArgumentException if `threshold` and `thresholds` are not equivalent
      */
    protected def checkThresholdConsistency(): Unit = {
        if (isSet(threshold) && isSet(thresholds)) {
            val ts = $(thresholds)
            require(ts.length == 2, "Logistic Regression found inconsistent values for threshold and" +
                    s" thresholds.  Param threshold is set (${$(threshold)}), indicating binary" +
                    s" classification, but Param thresholds is set with length ${ts.length}." +
                    " Clear one Param value to fix this problem.")
            val t = 1.0 / (1.0 + ts(0) / ts(1))
            require(math.abs($(threshold) - t) < 1E-5, "Logistic Regression getThreshold found" +
                    s" inconsistent values for threshold (${$(threshold)}) and thresholds (equivalent to $t)")
        }
    }

    override protected def validateAndTransformSchema(
                                                         schema: StructType,
                                                         fitting: Boolean,
                                                         featuresDataType: DataType): StructType =
    {
        checkThresholdConsistency()
        super.validateAndTransformSchema(schema, fitting, featuresDataType)
    }
}

/**
  * Binary Logistic Regression with double values for response variable (instead of usual 0/1 values).
  * Needed for Platt Scaling.
  */
class BinaryLogisticRegressionWithDoubleResponse(override val uid: String)
        extends ProbabilisticClassifier[Vector, BinaryLogisticRegressionWithDoubleResponse, BinaryLogisticRegressionWithDoubleResponseModel]
                with BinaryLogisticRegressionWithDoubleResponseParams with DefaultParamsWritable with Logging
{

    def this() = this(Identifiable.randomUID("binlogreg"))

    /**
      * Set the regularization parameter.
      * Default is 0.0.
      *
      * @group setParam
      */
    def setRegParam(value: Double): this.type = set(regParam, value)
    setDefault(regParam -> 0.0)

    /**
      * Set the ElasticNet mixing parameter.
      * For alpha = 0, the penalty is an L2 penalty.
      * For alpha = 1, it is an L1 penalty.
      * For alpha in (0,1), the penalty is a combination of L1 and L2.
      * Default is 0.0 which is an L2 penalty.
      *
      * Note: Fitting under bound constrained optimization only supports L2 regularization,
      * so throws exception if this param is non-zero value.
      *
      * @group setParam
      */
    def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)
    setDefault(elasticNetParam -> 0.0)

    /**
      * Set the maximum number of iterations.
      * Default is 100.
      *
      * @group setParam
      */
    def setMaxIter(value: Int): this.type = set(maxIter, value)
    setDefault(maxIter -> 100)

    /**
      * Set the convergence tolerance of iterations.
      * Smaller value will lead to higher accuracy at the cost of more iterations.
      * Default is 1E-6.
      *
      * @group setParam
      */
    def setTol(value: Double): this.type = set(tol, value)
    setDefault(tol -> 1E-6)

    /**
      * Whether to fit an intercept term.
      * Default is true.
      *
      * @group setParam
      */
    def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
    setDefault(fitIntercept -> true)

    /**
      * Whether to standardize the training features before fitting the model.
      * The coefficients of models will be always returned on the original scale,
      * so it will be transparent for users. Note that with/without standardization,
      * the models should be always converged to the same solution when no regularization
      * is applied. In R's GLMNET package, the default behavior is true as well.
      * Default is true.
      *
      * @group setParam
      */
    def setStandardization(value: Boolean): this.type = set(standardization, value)
    setDefault(standardization -> true)

    override def setThreshold(value: Double): this.type = super.setThreshold(value)
    setDefault(threshold -> 0.5)

    override def getThreshold: Double = super.getThreshold

    /**
      * Sets the value of param [[weightCol]].
      * If this is not set or empty, we treat all instance weights as 1.0.
      * Default is not set, so all instances have weight one.
      *
      * @group setParam
      */
    def setWeightCol(value: String): this.type = set(weightCol, value)

    private var optInitialModel: Option[BinaryLogisticRegressionWithDoubleResponseModel] = None

    private[spark] def setInitialModel(model: BinaryLogisticRegressionWithDoubleResponseModel): this.type = {
        this.optInitialModel = Some(model)
        this
    }

    override protected[spark] def train(dataset: Dataset[_]): BinaryLogisticRegressionWithDoubleResponseModel = {
        val handlePersistence = dataset.storageLevel == StorageLevel.NONE
        train(dataset, handlePersistence)
    }

    protected[spark] def train(
                                  dataset: Dataset[_],
                                  handlePersistence: Boolean): BinaryLogisticRegressionWithDoubleResponseModel = instrumented { instr =>
        val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
        val instances: RDD[Instance] =
            dataset.select(col($(labelCol)), w, col($(featuresCol))).rdd.map {
                case Row(label: Double, weight: Double, features: Vector) =>
                    Instance(label, weight, features)
            }

        if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

        instr.logPipelineStage(this)
        instr.logDataset(dataset)
        instr.logParams(this, regParam, elasticNetParam, standardization, threshold,
            maxIter, tol, fitIntercept)

        val (summarizer, labelSummarizer) = {
            val seqOp = (c: (MultivariateOnlineSummarizer, MultiClassSummarizerWithDoubleResponse),
                         instance: Instance) =>
                (c._1.add(instance.features, instance.weight), c._2.add(instance.label, instance.weight))

            val combOp = (c1: (MultivariateOnlineSummarizer, MultiClassSummarizerWithDoubleResponse),
                          c2: (MultivariateOnlineSummarizer, MultiClassSummarizerWithDoubleResponse)) =>
                (c1._1.merge(c2._1), c1._2.merge(c2._2))

            instances.treeAggregate(
                (new MultivariateOnlineSummarizer, new MultiClassSummarizerWithDoubleResponse)
            )(seqOp, combOp, $(aggregationDepth))
        }
        instr.logNumExamples(summarizer.count)
        instr.logNamedValue("lowestLabelWeight", labelSummarizer.histogram.min.toString)
        instr.logNamedValue("highestLabelWeight", labelSummarizer.histogram.max.toString)

        val histogram = labelSummarizer.histogram
        val numInvalid = labelSummarizer.countInvalid
        val numFeatures = summarizer.mean.size
        val numFeaturesPlusIntercept = if (getFitIntercept) numFeatures + 1 else numFeatures

        val numClasses = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
            case Some(n: Int) =>
                require(n >= histogram.length, s"Specified number of classes $n was " +
                        s"less than the number of unique labels ${histogram.length}.")
                n
            case None => histogram.length
        }

        val numCoefficientSets = 1

        if (isDefined(thresholds)) {
            require($(thresholds).length == numClasses, this.getClass.getSimpleName +
                    ".train() called with non-matching numClasses and thresholds.length." +
                    s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
        }

        instr.logNumClasses(numClasses)
        instr.logNumFeatures(numFeatures)

        val (coefficientMatrix, interceptVector, objectiveHistory) = {
            if (numInvalid != 0) {
                val msg = s"Classification labels should be in [0 to ${numClasses - 1}]. " +
                        s"Found $numInvalid invalid labels."
                instr.logError(msg)
                throw new SparkException(msg)
            }

            val isConstantLabel = histogram.count(_ != 0.0) == 1

            if ($(fitIntercept) && isConstantLabel) {
                instr.logWarning(s"All labels are the same value and fitIntercept=true, so the " +
                        s"coefficients will be zeros. Training is not needed.")
                val constantLabelIndex = Vectors.dense(histogram).argmax
                val coefMatrix = new SparseMatrix(numCoefficientSets, numFeatures,
                    new Array[Int](numCoefficientSets + 1), Array.empty[Int], Array.empty[Double],
                    isTransposed = true).compressed
                val interceptVec = Vectors.dense(if (numClasses == 2) Double.PositiveInfinity else Double.NegativeInfinity)
                (coefMatrix, interceptVec, Array.empty[Double])
            } else {
                if (!$(fitIntercept) && isConstantLabel) {
                    instr.logWarning(s"All labels belong to a single class and fitIntercept=false. It's a " +
                            s"dangerous ground, so the algorithm may not converge.")
                }

                val featuresMean = summarizer.mean.toArray
                val featuresStd = summarizer.variance.toArray.map(math.sqrt)

                if (!$(fitIntercept) && (0 until numFeatures).exists { i =>
                    featuresStd(i) == 0.0 && featuresMean(i) != 0.0 }) {
                    instr.logWarning("Fitting LogisticRegressionModel without intercept on dataset with " +
                            "constant nonzero column, Spark MLlib outputs zero coefficients for constant " +
                            "nonzero columns. This behavior is the same as R glmnet but different from LIBSVM.")
                }

                val regParamL1 = $(elasticNetParam) * $(regParam)
                val regParamL2 = (1.0 - $(elasticNetParam)) * $(regParam)

                val bcFeaturesStd = instances.context.broadcast(featuresStd)
                val getAggregatorFunc = new BinaryLogisticWithDoubleResponseAggregator(bcFeaturesStd, numClasses, $(fitIntercept), multinomial = false)(_)
                val getFeaturesStd = (j: Int) => if (j >= 0 && j < numCoefficientSets * numFeatures) {
                    featuresStd(j / numCoefficientSets)
                } else {
                    0.0
                }

                val regularization = if (regParamL2 != 0.0) {
                    val shouldApply = (idx: Int) => idx >= 0 && idx < numFeatures * numCoefficientSets
                    Some(new L2Regularization(regParamL2, shouldApply,
                        if ($(standardization)) None else Some(getFeaturesStd)))
                } else {
                    None
                }

                val costFun = new RDDLossFunction(instances, getAggregatorFunc, regularization,
                    $(aggregationDepth))

                val numCoeffsPlusIntercepts = numFeaturesPlusIntercept * numCoefficientSets

                val (lowerBounds, upperBounds): (Array[Double], Array[Double]) = (null, null)

                val optimizer = if ($(elasticNetParam) == 0.0 || $(regParam) == 0.0) {
                    if (lowerBounds != null && upperBounds != null) {
                        new BreezeLBFGSB(
                            BDV[Double](lowerBounds), BDV[Double](upperBounds), $(maxIter), 10, $(tol))
                    } else {
                        new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
                    }
                } else {
                    val standardizationParam = $(standardization)
                    def regParamL1Fun = (index: Int) => {
                        // Remove the L1 penalization on the intercept
                        val isIntercept = $(fitIntercept) && index >= numFeatures * numCoefficientSets
                        if (isIntercept) {
                            0.0
                        } else {
                            if (standardizationParam) {
                                regParamL1
                            } else {
                                val featureIndex = index / numCoefficientSets
                                // If `standardization` is false, we still standardize the data
                                // to improve the rate of convergence; as a result, we have to
                                // perform this reverse standardization by penalizing each component
                                // differently to get effectively the same objective function when
                                // the training dataset is not standardized.
                                if (featuresStd(featureIndex) != 0.0) {
                                    regParamL1 / featuresStd(featureIndex)
                                } else {
                                    0.0
                                }
                            }
                        }
                    }
                    new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))
                }

                /*
                  The coefficients are laid out in column major order during training. Here we initialize
                  a column major matrix of initial coefficients.
                 */
                val initialCoefWithInterceptMatrix =
                    Matrices.zeros(numCoefficientSets, numFeaturesPlusIntercept)

                val initialModelIsValid = optInitialModel match {
                    case Some(_initialModel) =>
                        val providedCoefs = _initialModel.coefficientMatrix
                        val modelIsValid = (providedCoefs.numRows == numCoefficientSets) &&
                                (providedCoefs.numCols == numFeatures) &&
                                (_initialModel.interceptVector.size == numCoefficientSets) &&
                                (_initialModel.getFitIntercept == $(fitIntercept))
                        if (!modelIsValid) {
                            instr.logWarning(s"Initial coefficients will be ignored! Its dimensions " +
                                    s"(${providedCoefs.numRows}, ${providedCoefs.numCols}) did not match the " +
                                    s"expected size ($numCoefficientSets, $numFeatures)")
                        }
                        modelIsValid
                    case None => false
                }

                if (initialModelIsValid) {
                    val providedCoef = optInitialModel.get.coefficientMatrix
                    providedCoef.foreachActive { (classIndex, featureIndex, value) =>
                        // We need to scale the coefficients since they will be trained in the scaled space
                        initialCoefWithInterceptMatrix.update(classIndex, featureIndex,
                            value * featuresStd(featureIndex))
                    }
                    if ($(fitIntercept)) {
                        optInitialModel.get.interceptVector.foreachActive { (classIndex, value) =>
                            initialCoefWithInterceptMatrix.update(classIndex, numFeatures, value)
                        }
                    }
                } else if ($(fitIntercept)) {
                    /*
                       For binary logistic regression, when we initialize the coefficients as zeros,
                       it will converge faster if we initialize the intercept such that
                       it follows the distribution of the labels.

                       {{{
                         P(0) = 1 / (1 + \exp(b)), and
                         P(1) = \exp(b) / (1 + \exp(b))
                       }}}, hence
                       {{{
                         b = \log{P(1) / P(0)} = \log{count_1 / count_0}
                       }}}
                     */
                    initialCoefWithInterceptMatrix.update(0, numFeatures,
                        math.log(histogram(1) / histogram(0)))
                }

                val states = optimizer.iterations(new CachedDiffFunction(costFun),
                    new BDV[Double](initialCoefWithInterceptMatrix.toArray))

                /*
                   Note that in Logistic Regression, the objective history (loss + regularization)
                   is log-likelihood which is invariant under feature standardization. As a result,
                   the objective history from optimizer is the same as the one in the original space.
                 */
                val arrayBuilder = mutable.ArrayBuilder.make[Double]
                var state: optimizer.State = null
                while (states.hasNext) {
                    state = states.next()
                    arrayBuilder += state.adjustedValue
                }
                bcFeaturesStd.destroy(blocking = false)

                if (state == null) {
                    val msg = s"${optimizer.getClass.getName} failed."
                    instr.logError(msg)
                    throw new SparkException(msg)
                }

                /*
                   The coefficients are trained in the scaled space; we're converting them back to
                   the original space.

                   Additionally, since the coefficients were laid out in column major order during training
                   to avoid extra computation, we convert them back to row major before passing them to the
                   model.

                   Note that the intercept in scaled space and original space is the same;
                   as a result, no scaling is needed.
                 */
                val allCoefficients = state.x.toArray.clone()
                val allCoefMatrix = new DenseMatrix(numCoefficientSets, numFeaturesPlusIntercept,
                    allCoefficients)
                val denseCoefficientMatrix = new DenseMatrix(numCoefficientSets, numFeatures,
                    new Array[Double](numCoefficientSets * numFeatures), isTransposed = true)
                //val interceptVec = Vectors.zeros(numCoefficientSets)
                val interceptVec = new Array[Double](numCoefficientSets)
                // separate intercepts and coefficients from the combined matrix
                allCoefMatrix.foreachActive { (classIndex, featureIndex, value) =>
                    val isIntercept = $(fitIntercept) && (featureIndex == numFeatures)
                    if (!isIntercept && featuresStd(featureIndex) != 0.0) {
                        denseCoefficientMatrix.update(classIndex, featureIndex,
                            value / featuresStd(featureIndex))
                    }
                    if (isIntercept) interceptVec(classIndex) = value
                }

                (denseCoefficientMatrix.compressed, Vectors.dense(interceptVec).compressed, arrayBuilder.result())
            }
        }

        if (handlePersistence) instances.unpersist()

        val model = copyValues(new BinaryLogisticRegressionWithDoubleResponseModel(uid, coefficientMatrix, interceptVector, numClasses))

        model
    }

    override def copy(extra: ParamMap): BinaryLogisticRegressionWithDoubleResponse = defaultCopy(extra)

}

object BinaryLogisticRegressionWithDoubleResponse extends DefaultParamsReadable[BinaryLogisticRegressionWithDoubleResponse]
{
    override def load(path: String): BinaryLogisticRegressionWithDoubleResponse = super.load(path)
}

/**
  * Model produced by [[BinaryLogisticRegressionWithDoubleResponse]].
  */
class BinaryLogisticRegressionWithDoubleResponseModel private[spark](
                                                     override val uid: String,
                                                     val coefficientMatrix: Matrix,
                                                     val interceptVector: Vector,
                                                     override val numClasses: Int)
        extends ProbabilisticClassificationModel[Vector, BinaryLogisticRegressionWithDoubleResponseModel]
                with BinaryLogisticRegressionWithDoubleResponseParams with MLWritable {

    require(coefficientMatrix.numRows == interceptVector.size, s"Dimension mismatch! Expected " +
            s"coefficientMatrix.numRows == interceptVector.size, but ${coefficientMatrix.numRows} != " +
            s"${interceptVector.size}")

    private[spark] def this(uid: String, coefficients: Vector, intercept: Double) =
        this(uid, new DenseMatrix(1, coefficients.size, coefficients.toArray, isTransposed = true),
            Vectors.dense(intercept), 2)

    /**
      * A vector of model coefficients for "binomial" logistic regression.
      *
      * @return Vector
      */
    def coefficients: Vector = _coefficients

    // convert to appropriate vector representation without replicating data
    private lazy val _coefficients: Vector = {
        require(coefficientMatrix.isTransposed,
            "LogisticRegressionModel coefficients should be row major for binomial model.")
        coefficientMatrix match {
            case dm: DenseMatrix => Vectors.dense(dm.values)
            case sm: SparseMatrix => Vectors.sparse(coefficientMatrix.numCols, sm.rowIndices, sm.values)
        }
    }

    /**
      * The model intercept for "binomial" logistic regression.
      *
      * @return Double
      */
    def intercept: Double = _intercept

    private lazy val _intercept = interceptVector.toArray.head

    /** Margin (rawPrediction) for class label 1.  For binary classification only. */
    private val margin: Vector => Double = (features) => {
        BLAS.dot(features, _coefficients) + _intercept
    }

    /** Margin (rawPrediction) for each class label. */
    private val margins: Vector => Vector = (features) => {
        val m = interceptVector.toDense.copy
        BLAS.gemv(1.0, coefficientMatrix, features, 1.0, m)
        m
    }

    /** Score (probability) for class label 1.  For binary classification only. */
    private val score: Vector => Double = (features) => {
        val m = margin(features)
        1.0 / (1.0 + math.exp(-m))
    }

    override val numFeatures: Int = coefficientMatrix.numCols

    /**
      * Predict label for the given feature vector.
      * The behavior of this can be adjusted using `thresholds`.
      */
    override def predict(features: Vector): Double = {
        // Note: We should use getThreshold instead of $(threshold) since getThreshold is overridden.
        if (score(features) > getThreshold) 1 else 0
    }

    override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
        rawPrediction match {
            case dv: DenseVector => new DenseVector(dv.values.map(v => 1.0 / (1.0 + math.exp(-v))))
            case sv: SparseVector =>
                throw new RuntimeException("Unexpected error in LogisticRegressionModel:" +
                        " raw2probabilitiesInPlace encountered SparseVector")
        }
    }

    override protected def predictRaw(features: Vector): Vector = {
        val m = margin(features)
        Vectors.dense(-m, m)
    }

    @Since("1.4.0")
    override def copy(extra: ParamMap): BinaryLogisticRegressionWithDoubleResponseModel = {
        val newModel = copyValues(new BinaryLogisticRegressionWithDoubleResponseModel(uid, coefficientMatrix, interceptVector, numClasses), extra)
        newModel.setParent(parent)
    }

    override protected def raw2prediction(rawPrediction: Vector): Double = {
        // Note: We should use getThreshold instead of $(threshold) since getThreshold is overridden.
        val t = getThreshold
        val rawThreshold = if (t == 0.0) {
            Double.NegativeInfinity
        } else if (t == 1.0) {
            Double.PositiveInfinity
        } else {
            math.log(t / (1.0 - t))
        }
        if (rawPrediction(1) > rawThreshold) 1 else 0
    }

    override protected def probability2prediction(probability: Vector): Double = {
            // Note: We should use getThreshold instead of $(threshold) since getThreshold is overridden.
            if (probability(1) > getThreshold) 1 else 0
    }

    /**
      * Returns a [[org.apache.spark.ml.util.MLWriter]] instance for this ML instance.
      *
      * This also does not save the [[parent]] currently.
      */
    @Since("1.6.0")
    override def write: MLWriter = new BinaryLogisticRegressionWithDoubleResponseModel.BinaryLogisticRegressionWithDoubleResponseModelWriter(this)

    override def toString: String = {
        s"LogisticRegressionModel: " +
                s"uid = ${super.toString}, numClasses = $numClasses, numFeatures = $numFeatures"
    }
}


object BinaryLogisticRegressionWithDoubleResponseModel extends MLReadable[BinaryLogisticRegressionWithDoubleResponseModel] {

    override def read: MLReader[BinaryLogisticRegressionWithDoubleResponseModel] = new BinaryLogisticRegressionWithDoubleResponseModelReader

    override def load(path: String): BinaryLogisticRegressionWithDoubleResponseModel = super.load(path)

    /** [[MLWriter]] instance for [[BinaryLogisticRegressionWithDoubleResponseModel]] */
    private[BinaryLogisticRegressionWithDoubleResponseModel]
    class BinaryLogisticRegressionWithDoubleResponseModelWriter(instance: BinaryLogisticRegressionWithDoubleResponseModel)
            extends MLWriter with Logging {

        private case class Data(
                                   numClasses: Int,
                                   numFeatures: Int,
                                   interceptVector: Vector,
                                   coefficientMatrix: Matrix)

        override protected def saveImpl(path: String): Unit = {
            // Save metadata and Params
            DefaultParamsWriter.saveMetadata(instance, path, sc)
            // Save model data: numClasses, numFeatures, intercept, coefficients
            val data = Data(instance.numClasses, instance.numFeatures, instance.interceptVector, instance.coefficientMatrix)
            val dataPath = new Path(path, "data").toString
            sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
        }
    }

    private class BinaryLogisticRegressionWithDoubleResponseModelReader extends MLReader[BinaryLogisticRegressionWithDoubleResponseModel] {

        /** Checked against metadata when loading model */
        private val className = classOf[BinaryLogisticRegressionWithDoubleResponseModel].getName

        override def load(path: String): BinaryLogisticRegressionWithDoubleResponseModel = {
            val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
            val (major, minor) = VersionUtils.majorMinorVersion(metadata.sparkVersion)

            val dataPath = new Path(path, "data").toString
            val data = sparkSession.read.format("parquet").load(dataPath)

            val model = if (major.toInt < 2 || (major.toInt == 2 && minor.toInt == 0)) {
                // 2.0 and before
                val Row(numClasses: Int, numFeatures: Int, intercept: Double, coefficients: Vector) =
                    MLUtils.convertVectorColumnsToML(data, "coefficients")
                            .select("numClasses", "numFeatures", "intercept", "coefficients")
                            .head()
                val coefficientMatrix =
                    new DenseMatrix(1, coefficients.size, coefficients.toArray, isTransposed = true)
                val interceptVector = Vectors.dense(intercept)
                new BinaryLogisticRegressionWithDoubleResponseModel(metadata.uid, coefficientMatrix, interceptVector, numClasses)
            } else {
                // 2.1+
                val Row(numClasses: Int, numFeatures: Int, interceptVector: Vector,
                coefficientMatrix: Matrix) = data
                        .select("numClasses", "numFeatures", "interceptVector", "coefficientMatrix").head()
                new BinaryLogisticRegressionWithDoubleResponseModel(metadata.uid, coefficientMatrix, interceptVector, numClasses)
            }

            metadata.getAndSetParams(model)
            model
        }
    }
}


/**
  * MultiClassSummarizerWithDoubleResponse computes the number of distinct labels and corresponding counts.
  * This class doesn't enforce that the labels have to be Integer.
  *
  * Two MultilabelSummarizer can be merged together to have a statistical summary of the
  * corresponding joint dataset.
  */
private[ml] class MultiClassSummarizerWithDoubleResponse extends Serializable
{
    // The first element of value in distinctMap is the actually number of instances,
    // and the second element of value is sum of the weights.
    private val distinctMap = new mutable.HashMap[Double, (Long, Double)]
    private var totalInvalidCnt: Long = 0L

    /**
      * Add a new label into this MultilabelSummarizer, and update the distinct map.
      *
      * @param label  The label for this data point.
      * @param weight The weight of this instances.
      * @return This MultilabelSummarizer
      */
    def add(label: Double, weight: Double = 1.0): this.type =
    {
        require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")

        if (weight == 0.0) return this

        val (counts: Long, weightSum: Double) = distinctMap.getOrElse(label, (0L, 0.0))
        distinctMap.put(label, (counts + 1L, weightSum + weight))
        this
    }

    /**
      * The total invalid input counts always returns zero.
      *
      * @return The total invalid input counts.
      */
    def countInvalid: Long = 0

    /**
      * Merge another MultilabelSummarizer, and update the distinct map.
      * (Note that it will merge the smaller distinct map into the larger one using in-place
      * merging, so either `this` or `other` object will be modified and returned.)
      *
      * @param other The other MultilabelSummarizer to be merged.
      * @return Merged MultilabelSummarizer object.
      */
    def merge(other: MultiClassSummarizerWithDoubleResponse): MultiClassSummarizerWithDoubleResponse =
    {
        val (largeMap, smallMap) = if (this.distinctMap.size > other.distinctMap.size)
        {
            (this, other)
        }
        else
        {
            (other, this)
        }
        smallMap.distinctMap.foreach
        {
            case (key, value) =>
                val (counts: Long, weightSum: Double) = largeMap.distinctMap.getOrElse(key, (0L, 0.0))
                largeMap.distinctMap.put(key, (counts + value._1, weightSum + value._2))
        }
        largeMap.totalInvalidCnt += smallMap.totalInvalidCnt
        largeMap
    }

    /** @return The number of distinct labels in the input dataset. */
    def numClasses: Int = if (distinctMap.isEmpty) 0
    else distinctMap.keySet.size

    /** @return The weightSum of each label in the input dataset. */
    def histogram: Array[Double] =
    {
        // The weightSums would be returned by the ascending order of label values.
        // Sort the keys and get the counts for each key.
        distinctMap
          .keys
                .toArray
                .sorted
                .map(distinctMap.getOrElse(_, (0L, 0.0))._2)
    }
}
