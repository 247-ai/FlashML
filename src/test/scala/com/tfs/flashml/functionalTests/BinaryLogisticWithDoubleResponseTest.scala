package com.tfs.flashml.functionalTests

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionWithDoubleResponse, BinaryLogisticRegressionWithDoubleResponseModel, LogisticRegression}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.Path
import scala.util.{Random, Try}

class BinaryLogisticWithDoubleResponseTest extends AnyFlatSpec
{
    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)

    println("=============================================================================================")
    println("Test case:  Logistic Regression with double values in the label column")

    val ss = SparkSession
            .builder()
            .appName("LRWithDoubleLabel")
            .config("spark.master", "local")
            .getOrCreate()

    import ss.implicits._

    "Saving and loading model" should "work" in
    {
         // Create a dataframe with random numbers
        val rand = new Random(2018)
        val (epsNeg, epsPos) = (0.003, 0.025)
        val df = (1 to 100)
                .foldLeft(new ArrayBuffer[(Double, Double)]())
                {
                    (accum, _) =>
                    {
                        val x = rand.nextDouble()
                        accum += (if(x < 0.5) (epsNeg, x) else (1.0 - epsPos, x))
                    }
                }
                .toDF("label", "x")
        val trdf = new VectorAssembler().setInputCols(Array("x")).setOutputCol("features").transform(df)
        val blrModel = new BinaryLogisticRegressionWithDoubleResponse().setMaxIter(10).setRegParam(0.1).setElasticNetParam(0.8)
                .fit(trdf)

        // Save this model
        val savePath = Path(s"/tmp/test_models/${this.getClass.getSimpleName}/SaveLoadModel")
        blrModel.write.overwrite().save(savePath.toString)

        withClue("Check model parameters: ")
        {
            // Now read the model
            val readModel = BinaryLogisticRegressionWithDoubleResponseModel.load(savePath.toString)

            assertResult(blrModel.interceptVector, "Model Intercept")(readModel.interceptVector)
            assertResult(blrModel.coefficientMatrix, "Model coefficients")(readModel.coefficientMatrix)
            assertResult(blrModel.interceptVector, "Model Intercept Vector")(readModel.interceptVector)
            assertResult(blrModel.coefficientMatrix, "Model Coeff Matrix")(readModel.coefficientMatrix)
        }

        // Clean up
        Try(Path("/tmp/test_models/").deleteRecursively())
    }

    "Saving and loading pipeline with the model" should "work" in
    {
         // Create a dataframe with random numbers
        val rand = new Random(2018)
        val (epsNeg, epsPos) = (0.003, 0.025)
        val df = (1 to 100)
                .foldLeft(new ArrayBuffer[(Double, Double)]())
                {
                    (accum, _) =>
                    {
                        val x = rand.nextDouble()
                        accum += (if(x < 0.5) (epsNeg, x) else (1.0 - epsPos, x))
                    }
                }
                .toDF("label", "x")
        val pipeline = new Pipeline().setStages(Array(
            new VectorAssembler().setInputCols(Array("x")).setOutputCol("features"),
            new BinaryLogisticRegressionWithDoubleResponse().setMaxIter(10).setRegParam(0.1).setElasticNetParam(0.8)
        ))

        val model = pipeline.fit(df)
        // Save the pipeline
        val savePath = Path(s"/tmp/test_models/${this.getClass.getSimpleName}/SaveLoadPipeline")
        model.write.overwrite().save(savePath.toString)

        withClue("Check pipeline parameters: ")
        {
            val readPipeline = PipelineModel.load(savePath.toString)

            assertResult(pipeline.getStages.length, "Count of stages")(readPipeline.stages.length)
            assertResult(true, "First transformer")(readPipeline.stages(0).isInstanceOf[VectorAssembler])
            assertResult(true, "Second transformer")(readPipeline.stages(1).isInstanceOf[BinaryLogisticRegressionWithDoubleResponseModel])
        }

        // Clean up
        Try(Path("/tmp/test_models/").deleteRecursively())
    }

    "Original LR output and New LR output" should "match" in
    {
        // Create a dataframe with random numbers
        val rand = new Random(2018)
        val df = (1 to 100)
                .foldLeft(new ArrayBuffer[(Double, Double)]())
                {
                    (accum, _) =>
                    {
                        val x = rand.nextDouble()
                        accum += (if(x < 0.5) (0.0d, x) else (1.0d, x))
                    }
                }
                .toDF("label", "x")
        val  trdf = new VectorAssembler().setInputCols(Array("x")).setOutputCol("features").transform(df)
        val origLRModel = new LogisticRegression().setMaxIter(10).setRegParam(0.1).setElasticNetParam(0.8)
                .fit(trdf)
        val origlrtr = origLRModel.transform(trdf)

        val newLRModel = new BinaryLogisticRegressionWithDoubleResponse().setMaxIter(10).setRegParam(0.1).setElasticNetParam(0.8)
                .fit(trdf)
        val newlrtr = newLRModel.transform(trdf)

        withClue("Check model objects: ")
        {
            assertResult(origLRModel.intercept, "Original Intercept Value")(newLRModel.intercept)
            assertResult(origLRModel.coefficients, "Original coefficients")(newLRModel.coefficients)
            assertResult(origLRModel.interceptVector, "Original Intercept Vector")(newLRModel.interceptVector)
            assertResult(origLRModel.coefficientMatrix, "Original Coeff Matrix")(newLRModel.coefficientMatrix)
        }

        withClue("Check prediction column: ")
        {
            assertResult(0)
            {
                origlrtr.join(newlrtr, origlrtr("x") === newlrtr("x"), "outer")
                        .withColumn("diff", origlrtr("prediction") - newlrtr("prediction"))
                        .filter($"diff" > 0)
                        .count()
            }
        }

        withClue("Check probabilty column: ")
        {
            assertResult(0)
            {
                // Define an UDF to extract the first value in the probability vector
                def vectorVal(idx: Int) = udf
                { x: org.apache.spark.ml.linalg.Vector => x(idx) }
                // Extract the first value from the probability columns of both DFs and compare.
                origlrtr.join(newlrtr, origlrtr("x") === newlrtr("x"), "outer")
                        .withColumn("diff", vectorVal(0)(origlrtr("probability")) - vectorVal(0)(newlrtr("probability")))
                        .filter($"diff" > 0)
                        .count()
            }
        }
    }

    "New LR output with Double Response" should "match" in
    {
        // Create a dataframe with random numbers
        val rand = new Random(2018)
        val (epsPos, epsNeg) = (0.003, 0.025)
        val df = (1 to 1000)
                .foldLeft(new ArrayBuffer[(Double, Double)]())
                {
                    (accum, _) =>
                    {
                        val x = rand.nextDouble()
                        accum += (if(x < 0.5) (epsNeg, x) else (1.0 - epsPos, x))
                    }
                }
                .toDF("label", "x")
        val  trdf = new VectorAssembler().setInputCols(Array("x")).setOutputCol("features").transform(df)
        val newlrtr = new BinaryLogisticRegressionWithDoubleResponse().setMaxIter(10).setRegParam(0.1).setElasticNetParam(0.8)
                .fit(trdf)
                .transform(trdf)

        val compareEps = 1e-10
        val expectedProbs = Array(0.31412320513349956, 0.48949021649970464, 0.8183129743672028, 0.7775919555912927, 0.09085128664222807, 0.16351755651104072, 0.8287906993888117, 0.06413223244964802, 0.3100120780964239, 0.1766930659644679)

        assertResult(0)
        {
            // Define an UDF to extract the first value in the probability vector
            def vectorVal(idx: Int) = udf{ x:org.apache.spark.ml.linalg.Vector => x(idx) }
            // Calculate the relative difference of this value and the expected value
            val actualProbs = newlrtr.select(vectorVal(0)($"probability")).take(10)
            expectedProbs
                    .zipWithIndex
                    .map{ case (v, i) => (actualProbs(i).get(0).asInstanceOf[Double] - v)/v }
                    .count(v => v > compareEps)
        }
    }

}
