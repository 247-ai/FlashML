package com.tfs.flashml.functionalTests

import com.tfs.flashml.TestUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.Path
import scala.util.Try

class PlattScalerWithTopKTest extends FlatSpec
{
    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)

    println("=============================================================================================")
    println("Test case:  Classification with SVM and Platt Scaling")

    val ss = SparkSession
            .builder()
            .appName("SVMWithPlattScaling")
            .config("spark.master", "local")
            .getOrCreate()

    import ss.implicits._

    "Binary classification with SVM and Platt Scaling" should "work" in
    {
        // Set up the Titanic dataset for binary classification
        val source: String = s"${TestUtils.getDataFolder}/titanic-survival-data.csv.gz"
        val df = ss.read.option("header", "true").csv(source)
                .select("survived", "age", "home_dest")
                .filter($"age".isNotNull && $"home_dest".isNotNull)
                .withColumn("age", $"age".cast(DoubleType))
                .withColumn("survived", $"survived".cast(DoubleType))
                .withColumnRenamed("survived", "label")

        val pipeline = new Pipeline().setStages(Array(
            // Tokenize home_dest
            // Ideally, we should do some more cleanup, like removal of "," and "/" etc
            new RegexTokenizer().setInputCol("home_dest").setOutputCol("home_dest_tokenized").setPattern("\\W").setToLowercase(false),
            // Generate 2-grams
            new NGram().setN(2).setInputCol("home_dest_tokenized").setOutputCol("bigrams"),
            // Next generate HashingTF from the gram columns
            new HashingTF().setInputCol("home_dest_tokenized").setOutputCol("ughash").setNumFeatures(2000),
            new HashingTF().setInputCol("bigrams").setOutputCol("bghash").setNumFeatures(2000),
            new VectorAssembler().setInputCols(Array("age", "ughash", "bghash")).setOutputCol("features"),
            new LinearSVC().setMaxIter(10).setRegParam(0.1),
            new PlattScalar().setIsMultiIntent(false).setLabelCol("label")
        ))
        val model = pipeline.fit(df)
        val first20Preds = model.transform(df).select("prediction").take(20)
        val expected = Array(1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0)
        expected.zipWithIndex.foreach
        {
            case(v, i) => assertResult(v, s"Value at row $i")(first20Preds(i).get(0).asInstanceOf[Double])
        }
    }

    "Multiclass classification with SVM and Platt Scaling" should "work" in
    {
        // Set up the yelp dataset for multi-class classification
        val source: String = s"${TestUtils.getDataFolder}/yelp-data/reviews_1k.json.gz"
        val df = ss.read.json(source)
                .select("stars", "text")
                .withColumn("length", length($"text"))
        //df.show()

        val trainingPipeline = new Pipeline().setStages(Array(
            // Set up a StringIndexer for the response column
            new StringIndexer().setInputCol("stars").setOutputCol("label"),
            // Tokenize text
            new RegexTokenizer().setInputCol("text").setOutputCol("text_tokenized").setPattern("\\W").setToLowercase(false),
            // Generate 2-grams
            new NGram().setN(2).setInputCol("text_tokenized").setOutputCol("bigrams"),
            // Next generate HashingTF from the gram columns
            new HashingTF().setInputCol("text_tokenized").setOutputCol("ughash").setNumFeatures(2000),
            new HashingTF().setInputCol("bigrams").setOutputCol("bghash").setNumFeatures(2000),
            //  Next combine these vectors using VectorAssembler
            new VectorAssembler().setInputCols(Array("length", "ughash", "bghash")).setOutputCol("features"),
            // Now run an One-Vs-Rest SVM model
            new OneVsRest().setClassifier(new LinearSVC().setMaxIter(10).setRegParam(0.1)),
            new PlattScalar().setIsMultiIntent(true).setLabelCol("label"),
            // Finally, we need to convert the prediction back to own labels
            new IndexToString().setInputCol("prediction").setOutputCol("result")
        ))
        val trainingPipelineModel = trainingPipeline.fit(df)
        val transformed = trainingPipelineModel.transform(df)

        // Check the predicted column with fixed result for the first 20 rows
        val first20Preds = transformed
                .select("result")
                .take(20)
        val expected = Array(2, 5, 1, 2, 5, 1, 5, 5, 4, 2, 5, 1, 4, 4, 5, 4, 5, 5, 5, 5)
        expected.zipWithIndex.foreach
        {
            case(v, i) => assertResult(v.toString, s"(Value at row ${i+1} [1-based])")(first20Preds(i).get(0).asInstanceOf[String])
        }

        // Calculate TopK Intents
        val topKPipeline = new Pipeline().setStages(Array(
            new TopKIntents().setStringIndexerModel(trainingPipelineModel.stages(0).asInstanceOf[StringIndexerModel]).setKValue(3).setOutputCol("topKIntents")
        ))
        val topKPipelineModel = topKPipeline.fit(transformed)
        val dfWithTopK = topKPipelineModel.transform(transformed).select("topKIntents")

        // Extract the topK columns
        val resultCols = dfWithTopK
            .take(10)
            .map(row =>
            {
                // Cast the 0-th element of this row as a sequence of rows
                val subRows = row.getSeq[Row](0)
                // Now collect the data from these subrows
                subRows.foldLeft(ArrayBuffer[(String, Double)]())
                {
                    case (accum, value) => accum :+ (value.getString(0), value.getDouble(1))
                }.toArray
            })

        // Check the result for the first 10 rows
        val expectedTopK = Array(
            Array((2, 0.5408867688043552), (5, 0.19278032158335726), (1, 0.11090739711220278)),
            Array((5, 0.41023876601122555), (4, 0.15929685439067098), (2, 0.14339595736593877)),
            Array((1, 0.41671868315559074), (3, 0.15644442842386722), (4, 0.1296361367857409)),
            Array((2, 0.5581861176618106), (3, 0.18706408868537522), (1, 0.1597521941194581)),
            Array((5, 0.3444060397723536), (4, 0.20073029347562665), (3, 0.1602489294083707)),
            Array((1, 0.443814313094639), (2, 0.13751813394257317), (5, 0.12854181782618124)),
            Array((5, 0.4070238247528128), (4, 0.14026845647363687), (1, 0.13049169804183536)),
            Array((5, 0.3999038911722893), (4, 0.11999424270018082), (2, 0.09186262394281214)),
            Array((4, 0.2358652298266914), (5, 0.16007935635969503), (3, 0.13778260539666215)),
            Array((2, 0.6694712353456262), (1, 0.16247296388932356), (5, 0.15650943913033122))
        )
        val compareEps = 1e-10
        expectedTopK.zip(resultCols).foreach
        {
            case(ev, av) => ev
                .map(v => (v._1.toString, v._2))
                .zip(av)
                .foreach
                {
                    case (ec, ac) =>
                        assertResult(ec._1, "Expected result [TopK] - Label")(ac._1)
                        val relativeErr = Math.abs((ac._2 - ec._2) / ec._2)
                        assertResult(true, "Expected result [TopK] - Probability")(relativeErr < compareEps)
                }
        }

        // Save the pipeline and load it back
        trainingPipelineModel.write.overwrite().save(s"/tmp/test_models/${this.getClass.getSimpleName}/TrainingPipelineModel/")
        topKPipelineModel.write.overwrite().save(s"/tmp/test_models/${this.getClass.getSimpleName}/TopKPipelineModel/")

        withClue("Check pipeline parameters")
        {
            val readTrainPipeline = PipelineModel.load(s"/tmp/test_models/${this.getClass.getSimpleName}/TrainingPipelineModel/")
            val readTopKPipeline = PipelineModel.load(s"/tmp/test_models/${this.getClass.getSimpleName}/TopKPipelineModel/")

            assertResult(trainingPipeline.getStages.length, "Count of stages")(readTrainPipeline.stages.length)
            assertResult(topKPipeline.getStages.length, "Count of stages")(readTopKPipeline.stages.length)
        }

        // Clean up
        Try(Path("/tmp/test_models/").deleteRecursively())
    }
}