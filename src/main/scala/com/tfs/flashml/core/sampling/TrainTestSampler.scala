package com.tfs.flashml.core.sampling

import java.text.DecimalFormat

import com.tfs.flashml.core.Validator
import com.tfs.flashml.util.conf.{ConfigValidatorException, FlashMLConstants}
import com.tfs.flashml.util.{ConfigValues, FlashMLConfig}
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Methods for sampling the data to divide into train and test dataset. Also contains methods
 * for checking class imbalance and rebalancing datasets.
 *
 * @since 4/17/17.
 */
object TrainTestSampler extends Validator
{
    private val log = LoggerFactory.getLogger(getClass)
    private val requiredMinorityClassPercent: Int = FlashMLConfig.getInt(FlashMLConstants.MINORITY_CLASS_PERCENT)
    private val seed = FlashMLConstants.SEED
    var minorityClassLabel: Any = _
    var majorityClassLabel: Any = _

    def sample(odf: Option[DataFrame]): Option[Array[DataFrame]] =
    {

        odf.map(df =>
        {
            // This is used for positive class validation and data balance
            if (ConfigValues.isSingleIntent)
            {
                val labels: Array[_] = findResponseColumnLabels(df)
                minorityClassLabel = labels(0)
                majorityClassLabel = labels(1)
            }

            val samplingType = FlashMLConfig
                    .getString(FlashMLConstants.SAMPLING_TYPE)
                    .toLowerCase

            samplingType match
            {
                case FlashMLConstants.SAMPLING_TYPE_RANDOM => randomSample(df)
                case FlashMLConstants.SAMPLING_TYPE_CONDITIONAL => conditionalSample(df)
                case FlashMLConstants.SAMPLING_TYPE_STRATIFIED => stratifiedSample(df)
            }

        })
    }

    /**
     * Sampling based on random number.
     *
     * @param df
     * @return
     */
    private def randomSample(df: DataFrame): Array[DataFrame] =
    {
        log.info("Generating train/test datasets using random sampling.")
        val split = FlashMLConfig.getIntArray(FlashMLConstants.SAMPLE_SPLIT).map(_.toDouble)
        df.randomSplit(split, seed = seed)
    }

    /**
     * Method to stabilize the response class.
     *
     * @param df
     * @return
     */
    private def responseClassStabilizer(df: DataFrame): DataFrame =
    {
        // Define a method to update a row with the new class string
        def update(row: Row, index: Int): Row =
        {
            val rowData = row.toSeq
            Row.fromSeq((rowData.take(index) :+ FlashMLConfig.getString(FlashMLConstants.OTHER_CLASS_VALUE)) ++
                    rowData.drop(index + 1))
        }

        // Get the index of the response variable
        val index = df
                .schema
                .fieldIndex(FlashMLConfig.getString(FlashMLConstants.RESPONSE_VARIABLE))
        val minClassSupport = FlashMLConfig.getInt(FlashMLConstants.MINIMUM_CLASS_SUPPORT)
        log.info(s"Stabilizing response class: class having example count lower than $minClassSupport will be updated" +
                s" to class: ${FlashMLConfig.getString(FlashMLConstants.OTHER_CLASS_VALUE)}")

        // Calculate a tuple containing (row data, count of class in the row)
        val temp = df.rdd.keyBy(_.getString(index))
        val count = temp.mapValues(_ => 1).reduceByKey(_ + _)
        val result = temp.join(count).values.map
        {
            case ((x), z) => (x, z)
        }
        val resRDD = result.map(x =>
        {
            if (x._2 < minClassSupport)
                update(x._1, index)
            else
                x._1
        })
        SparkSession.builder().getOrCreate().createDataFrame(resRDD, df.schema)
    }

    /**
     * Method to create a stratified sample through Spark RDD function
     *
     * @param inputDF
     * @return
     */
    private def stratifiedSample(inputDF: DataFrame): Array[DataFrame] =
    {
        //TODO use StratifiedTrainTestSplitter
        log.info("Generating train/test datasets using stratified sampling.")
        val df = if (FlashMLConfig.getBool(FlashMLConstants.MINIMUM_CLASS_SUPPORT_REQUIRED).equals(false))
        {
            inputDF
        }
        else
        {
            responseClassStabilizer(inputDF)
        }
        val index = df.schema.fieldIndex(FlashMLConfig.getString(FlashMLConstants.RESPONSE_VARIABLE))
        val sampleSplit = FlashMLConfig.getIntArray(FlashMLConstants.SAMPLE_SPLIT).map(_.toDouble)
        val trainPercent = new DecimalFormat("#.#").format(sampleSplit(0) * 0.01).toDouble
        val inputRDD = df
                .rdd
                .keyBy(_.get(index))

        inputRDD.cache()
        val fractions = inputRDD.map(_._1).distinct.map(x => (x, trainPercent)).collectAsMap()
        val outputRDD = inputRDD.sampleByKeyExact(false, fractions, seed = seed).values
        val trainDF = SparkSession.builder().getOrCreate().createDataFrame(outputRDD, df.schema)
        inputRDD.unpersist()

        if (sampleSplit.length == 2)
        {
            val testDF = df.except(trainDF)
            Array(trainDF, testDF)
        }
        else
            Array(trainDF)
    }

    /**
     * Method to sample the dataset to train and test based on certain condition. Note that all the conditions needs
     * to be specified in the config parameter.
     *
     * @param df
     * @return
     */
    private def conditionalSample(df: DataFrame): Array[DataFrame] =
    {
        log.info("Generating train/test datasets using conditional sampling.")
        val sampleConditions = FlashMLConfig.getStringArray(FlashMLConstants.SAMPLE_CONDITION)
        sampleConditions.map(df.filter)
    }

    /**
     * Method to validate the minimum required support of the positive class.
     *
     * Through testing, it was found that spark is unable to build a model if the positive class % is below a certain
     * threshold. The function checks the positive class % in the input data and gives a warning, if necessary.
     *
     * @param df : The dataframe to be validated
     */
    def validateMinorityClass(df: DataFrame): Unit =
    {
        if (FlashMLConfig.getInt(FlashMLConstants.MINORITY_CLASS_PERCENT).equals(0))
        {
            // Check class imbalance for binary classification problems
            if (FlashMLConfig.getString(FlashMLConstants.BUILD_TYPE).toLowerCase == FlashMLConstants
                    .BUILD_TYPE_BINOMIAL)
            {
                val minorityClassCount: Double = df.filter(s"${ConfigValues.responseColumn} == $minorityClassLabel")
                        .count()
                val totalCount: Double = df.count()
                if (minorityClassCount / totalCount < FlashMLConstants.MINORITY_CLASS_THRESHOLD)
                {
                    log.warn("WARNING: The % of minority class is less than 0.2%. Model cannot be built. Please use " +
                            "Data Balance option")
                }
            }
        }
        else if (requiredMinorityClassPercent < FlashMLConstants.MINORITY_CLASS_THRESHOLD * 100)
        {
            log.warn("WARNING: The % of minority class has to be greater than 0.2%. Please adjust the data balance " +
                    "parameters")
        }
    }


    /**
     * The function allows the user to modify the positive class % in the data for model building purpose. If we need to
     * reduce the positive class %, the negative class is oversampled by duplicating the data. If we need to increase
     * the
     * positive class %, the negative class is undersampled.
     *
     * @param df           : The input datframe which needs to be balanced
     * @param samplingType : The type of sampling i.e.random,conditional
     * @return The balanced dataframe
     */
    def dataBalance(df: DataFrame, samplingType: String): DataFrame =
    {
        var balancedDf: DataFrame = null
        val dfMin = df.filter(s"${ConfigValues.responseColumn} == $minorityClassLabel")
        val dfMaj = df.filter(s"${ConfigValues.responseColumn} == $majorityClassLabel")

        val minorityClassCount: Double = dfMin.count()
        val totalCount: Double = df.count()
        val minorityClassProportion = minorityClassCount / totalCount * 100

        if (samplingType.toLowerCase == FlashMLConstants.SAMPLING_TYPE_RANDOM)
        {
            if (minorityClassProportion < requiredMinorityClassPercent.toInt)
            {
                log.info(s"Minority Class: Required = $requiredMinorityClassPercent% Actual = " +
                        s"$minorityClassProportion% => Majority Class Undersampled")
                val dfMajUndersampled = dfMaj.sample(true, minorityClassCount / (totalCount - minorityClassCount) *
                        (100 -
                        requiredMinorityClassPercent.toInt) / requiredMinorityClassPercent.toInt, seed = seed)
                balancedDf = dfMin.union(dfMajUndersampled)
            }
            else if (minorityClassProportion > requiredMinorityClassPercent.toInt)
            {
                log.info(s"Minority Class: Required = $requiredMinorityClassPercent% Actual = " +
                        s"$minorityClassProportion% => Majority Class Oversampled")
                var dfMajMultiplier = minorityClassProportion * totalCount / requiredMinorityClassPercent.toInt /
                        (totalCount
                        - minorityClassCount)
                var dfMajOversampled = dfMaj
                while (dfMajMultiplier >= 2)
                {
                    dfMajOversampled = dfMajOversampled.union(dfMaj)
                    dfMajMultiplier = dfMajMultiplier - 1
                }
                val dfMajUndersampled = dfMaj.sample(true, dfMajMultiplier - 1, seed = seed)
                balancedDf = dfMajOversampled.union(dfMajUndersampled).union(dfMin)
            }
            else balancedDf = df
        }
        else if (samplingType.toLowerCase() == FlashMLConstants.SAMPLING_TYPE_CONDITIONAL)
        {
            if (minorityClassProportion < requiredMinorityClassPercent.toInt)
            {
                log.info(s"Minority Class: Required = $requiredMinorityClassPercent% Actual = " +
                        s"$minorityClassProportion% => Majority Class Undersampled")

                val rvMin = dfMaj.agg(min(dfMaj.col(ConfigValues.randomVariable))).head().getDouble(0)
                val rvMax = dfMaj.agg(max(dfMaj.col(ConfigValues.randomVariable))).head().getDouble(0)

                val dfMajUndersampled = dfMaj.filter(s"${ConfigValues.randomVariable} < ${
                    rvMin + (minorityClassCount / (totalCount -
                            minorityClassCount) * (100 - requiredMinorityClassPercent.toInt) /
                            requiredMinorityClassPercent.toInt *
                            (rvMax - rvMin))
                }")
                balancedDf = dfMin.union(dfMajUndersampled)
            }
            else if (minorityClassProportion > requiredMinorityClassPercent.toInt)
            {
                log.info(s"Minority Class: Required = $requiredMinorityClassPercent% Actual = " +
                        s"$minorityClassProportion% => Majority Class Oversampled")
                val rvMin = dfMaj.agg(min(dfMaj.col(ConfigValues.randomVariable))).head().getDouble(0)
                val rvMax = dfMaj.agg(max(dfMaj.col(ConfigValues.randomVariable))).head().getDouble(0)
                var dfMajMultiplier = minorityClassProportion * totalCount / requiredMinorityClassPercent.toInt /
                        (totalCount - minorityClassCount)
                var dfMajOversampled = dfMaj
                while (dfMajMultiplier > 2)
                {
                    dfMajOversampled = dfMajOversampled.union(dfMaj)
                    dfMajMultiplier = dfMajMultiplier - 1
                }
                val dfMajUndersampled = dfMaj.filter(s"${ConfigValues.randomVariable} < ${rvMin + ((dfMajMultiplier -
                        1) * (rvMax - rvMin))}")
                balancedDf = dfMajOversampled.union(dfMajUndersampled).union(dfMin)
            }
            else balancedDf = df
        }
        balancedDf
    }

    /**
     * Find the minority and majority class labels
     * Applicable only for single intent models
     *
     * @return Arrayof 2 elements where 0 index is minority label and 1 index is majority label
     */
    def findResponseColumnLabels(df: DataFrame): Array[_] =
    {
        df
                .groupBy(ConfigValues.responseColumn)
                .count()
                .orderBy("count")
                .select(ConfigValues.responseColumn)
                .collect
                .map(_.get(0))
    }


    /**
     * Restrict stratified sampling type to multi intent classifiers.
     */
    override def validate(): Unit =
    {
        val samplingType = FlashMLConfig
                .getString(FlashMLConstants.SAMPLING_TYPE)
                .toLowerCase

        if (ConfigValues.isSingleIntent && samplingType.equals(FlashMLConstants.SAMPLING_TYPE_STRATIFIED))
        {
            val msg = s"Sampling Type: $samplingType is not supported for single intent classifiers"
            log.error(msg)
            throw new ConfigValidatorException(msg)
        }
    }

}
