package com.tfs.flashml.functionalTests

import com.tfs.flashml.core.preprocessing.PreprocessingStageLoader
import com.tfs.flashml.core.preprocessing.transformer._
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemoverCustom}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.{col, concat}
import java.util.ArrayList

import com.tfs.flashml.TestUtils

import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
 * Class to test the functionality of preprocessing transformers for all FlashML supported preprocessing methods
 * Yelp dataset of 1000 rows used, loaded as a local JSON file
 */
class PreprocessingTest extends AnyFlatSpec
{
  private val log = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  println("=============================================================================================")
  log.info("Starting FlashML test application")
  println("Test case: Preprocessing methods on input yelp dataset")

  val ss: SparkSession = SparkSession.builder()
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "com.tfs.flashml.util.FlashMLKryoRegistrator")
    .config("spark.kryoserializer.buffer.max", "256")
    .config("spark.sql.parquet.compression.codec", "gzip")
    .master("local[*]")
    .appName("YelpDataset_test")
    .getOrCreate()

  import ss.implicits._

  "Preprocessing outputs" should "match" in
    {
      val source: String = s"${TestUtils.getDataFolder}/yelp-data/reviews_1k.json.gz"

      val primaryKey = "review_id"

      val data: DataFrame = ss
        .read
        .json(source)
        .select("text", primaryKey)

      val tokenizer = new RegexTokenizer()
        .setInputCol("text")
        .setOutputCol("tokenizedText")
        .setPattern(FlashMLConstants.CUSTOM_DELIMITER)
        .setToLowercase(false)

      val tokenizedText = tokenizer.transform(data)

      val stopwordsArray = Array(
        "k_class_number_ref",
        "i_class_phone_number",
        "n_class_esn",
        "d_class_time",
        "f_class_number _class_currency",
        "e_class_percentage",
        "a_class_email",
        "c_class_date",
        "h_class_phone_number",
        "m_class_radio_id",
        "j_class_number",
        "g_class_masked_acc_or_model_number",
        "b_class_url",
        "uh",
        "um",
        "is",
        "have",
        "has",
        "had",
        "was",
        "were",
        "are",
        "can",
        "could",
        "should",
        "shall",
        "oh",
        "shit",
        "i",
        "me",
        "my",
        "myself",
        "we",
        "our",
        "ours",
        "ourselves",
        "you",
        "your",
        "yours",
        "yourself",
        "yourselves",
        "he",
        "him",
        "his",
        "himself",
        "she",
        "her",
        "hers",
        "herself",
        "it",
        "its",
        "itself",
        "they",
        "them",
        "their",
        "theirs",
        "themselves",
        "what",
        "which",
        "who",
        "whom",
        "this",
        "that",
        "these",
        "those",
        "am",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "have",
        "has",
        "had",
        "having",
        "do",
        "does",
        "did",
        "doing",
        "a",
        "an",
        "the",
        "and",
        "but",
        "if",
        "or",
        "because",
        "as",
        "until",
        "while",
        "of",
        "at",
        "by",
        "for",
        "with",
        "about",
        "against",
        "between",
        "into",
        "through",
        "during",
        "before",
        "after",
        "above",
        "below",
        "to",
        "from",
        "up",
        "down",
        "in",
        "out",
        "on",
        "off",
        "over",
        "under",
        "again",
        "further",
        "then",
        "once",
        "here",
        "there",
        "when",
        "where",
        "why",
        "how",
        "all",
        "any",
        "both",
        "each",
        "few",
        "more",
        "most",
        "other",
        "some",
        "such",
        "no",
        "nor",
        "not",
        "only",
        "own",
        "same",
        "so",
        "than",
        "too",
        "very",
        "s",
        "t",
        "can",
        "will",
        "just",
        "don",
        "should",
        "now",
        "i'll",
        "you'll",
        "he'll",
        "she'll",
        "we'll",
        "they'll",
        "i'd",
        "you'd",
        "he'd",
        "she'd",
        "we'd",
        "they'd",
        "i'm",
        "you're",
        "he's",
        "she's",
        "it's",
        "we're",
        "they're",
        "i've",
        "we've",
        "you've",
        "they've",
        "isn't",
        "aren't",
        "wasn't",
        "weren't",
        "haven't",
        "hasn't",
        "hadn't",
        "don't",
        "doesn't",
        "didn't",
        "won't",
        "wouldn't",
        "shan't",
        "shouldn't",
        "mustn't",
        "can't",
        "couldn't",
        "cannot",
        "could",
        "here's",
        "how's",
        "let's",
        "ought",
        "that's",
        "there's",
        "what's",
        "when's",
        "where's",
        "who's",
        "why's",
        "would"
      )

      val stopWordsFiltered: StopWordsRemoverCustom = new StopWordsRemoverCustom()
        .setInputCol("text")
        .setOutputCol("stopWordFiltered")
        .setStopWords(stopwordsArray)
        .setDelimiter(FlashMLConstants.CUSTOM_DELIMITER)

      val stopwordFilteredComputedDF = stopWordsFiltered.transform(tokenizedText).drop("text",
        "tokenizedText")

      val contractionRepl = Map(
        "we're" -> "we are",
        "i'll" -> "i will",
        "he'll" -> "he will",
        "transaction's" -> "transaction has",
        "you'll" -> "you will",
        "they're" -> "they are",
        "advisor's" -> "advisor",
        "when's" -> "when is",
        "they'd" -> "they would",
        "ya'all" -> "you all",
        "you're" -> "you are",
        "it's" -> "it is",
        "won't" -> "will not",
        "we'd" -> "we would",
        "check's" -> "check has",
        "o'clock" -> "clock",
        "she'll" -> "she will",
        "account's" -> "account",
        "what're" -> "what are",
        "wouldn't" -> "would not",
        "haven't" -> "have not",
        "they've" -> "they have",
        "she's" -> "she is",
        "what's" -> "what is",
        "who'll" -> "who will",
        "who're" -> "who are",
        "can't" -> "cannot",
        "couldn't" -> "could not",
        "we'll" -> "we will",
        "'em" -> "them",
        "weren't" -> "were not",
        "when'll" -> "when will",
        "hadn't" -> "had not",
        "mightn't" -> "might not",
        "who's" -> "who is",
        "who'd" -> "who would",
        "let's" -> "let us",
        "deposit's" -> "deposit",
        "isn't" -> "is not",
        "somebody's" -> "somebody is",
        "didn't" -> "did not",
        "ain't" -> "am not",
        "shouldn't" -> "should not",
        "you've" -> "you have",
        "aren't" -> "are not",
        "we've" -> "we have",
        "what've" -> "what have",
        "doesn't" -> "does not",
        "class_debit_card's" -> "class_debit_card is",
        "today's" -> "today",
        "i'd" -> "i would",
        "wasn't" -> "was not",
        "there's" -> "there is",
        "hasn't" -> "has not",
        "he's" -> "he has",
        "don't" -> "do not",
        "shan't" -> "shall not",
        "mustn't" -> "must not",
        "he'd" -> "he would",
        "i've" -> "i have",
        "cashier's" -> "cashier",
        "y'all" -> "you all",
        "where's" -> "where is",
        "she'd" -> "she would",
        "you'd" -> "you would",
        "card's" -> "card is",
        "who've" -> "who have",
        "that's" -> "that is",
        "i'm" -> "i am",
        "what'll" -> "what will",
        "they'll" -> "they will"
      )


      val contractionsReplacement = new WordSubstitutionTransformer()
        .setInputCol("text")
        .setOutputCol("contractionReplacement")
        .setDictionary(contractionRepl)
        .setDelimiter(FlashMLConstants.CUSTOM_DELIMITER)

      val contractionReplComputedDF = contractionsReplacement.transform(tokenizedText).drop("text",
        "tokenizedText")


      val caseNormalizer = new CaseNormalizationTransformer()
        .setInputCol("text")
        .setOutputCol("caseNormalizationText")

      val caseNormalizedComputedDF = caseNormalizer.transform(tokenizedText).drop("text", "tokenizedText")


      val sentenceMarker = new SentenceMarker()
        .setInputCol("text")
        .setOutputCol("SentenceMarker")

      val sentenceMarkedComputedDF = sentenceMarker.transform(tokenizedText).drop("text", "tokenizedText")

      val wordClasses: Seq[(String, String)] = PreprocessingStageLoader
        .getWordClassReplacements(s"${TestUtils.getSupportFileFolder}/word_classes.txt").sortBy(-_
        ._1.length)
      val wordRegexSeq = PreprocessingStageLoader.computeWordRegexSeq(wordClasses)

      val wordClassReplTransformer = new RegexReplacementTransformer()
        .setInputCol("text")
        .setOutputCol("wordClassRepl")
        .setRegexReplacements(wordRegexSeq)

      val wordClassReplComputedDF = wordClassReplTransformer.transform(tokenizedText).drop("text", "tokenizedText")

      val exceptions = Array(
        "bought",
        "cancelled",
        "canceled",
        "closed",
        "delivered",
        "delivery",
        "discuss",
        "existing",
        "goods",
        "helping",
        "higher",
        "issuing",
        "opened",
        "opening",
        "paid",
        "paying",
        "pass",
        "posted",
        "posting",
        "removed",
        "setting",
        "settings",
        "sold")

      val stemmingTransformer = new PorterStemmingTransformer()
        .setInputCol("text")
        .setOutputCol("stemmingText")
        .setExceptions(exceptions)
        .setDelimiter(FlashMLConstants.CUSTOM_DELIMITER)

      val stemmingComputedDF = stemmingTransformer.transform(tokenizedText).drop("text", "tokenizedText")

      val regexClasses = Seq(
        Tuple2("_class_hello", "\\b(hola|hello|hi|hey)\\b")
      )

      val regexRemovals = Seq(
        Tuple2("", "_class_hello")
      )

      val regexReplTransformer = new RegexReplacementTransformer()
        .setInputCol("text")
        .setOutputCol("RegexRepl")
        .setRegexReplacements(regexClasses)
      val regexRemovalTransformer = new RegexReplacementTransformer()
        .setInputCol("text")
        .setOutputCol("regexRemovalText")
        .setRegexReplacements(regexRemovals)

      val regexReplComputedDF = regexReplTransformer.transform(tokenizedText).drop("text", "tokenizedText")

      val regexRemovalComputedDF = regexRemovalTransformer.transform(tokenizedText).drop("text", "tokenizedText")

      val resultsDF = stopwordFilteredComputedDF
        .join(stemmingComputedDF, "review_id")
        .join(regexReplComputedDF, "review_id")
        .join(regexRemovalComputedDF, "review_id")
        .join(caseNormalizedComputedDF, "review_id")
        .join(sentenceMarkedComputedDF, "review_id")
        .join(wordClassReplComputedDF, "review_id")
        .join(contractionReplComputedDF, "review_id")

      //resultsDF.write.option("compression","gzip").json("hdfs://localhost:9000/user/yelpResultDataset.json")
      val resultsDatasetLocation = s"${TestUtils.getDataFolder}/yelp-data/yelpResultDataset.json.gz"
      //val resultsDatasetLocation = "src/test/resources/yelpResultDataset.json.gz"

      val solutionsKeyPair = ss.read.json(resultsDatasetLocation)

      /**
       *
       * @param computedDF     DataFrame containing values computed currently by the Spark program
       * @param storedResultDF DataFrame loaded from a file location. Contains correct values for all the preprocessing methods.
       * @param primaryKey     String that points to the column that is a primary key for both the above DFs
       * @param checkingCol    value, it is the column present in both the above DFs that contains the values to be checked
       */
      def compareValues(computedDF: DataFrame, storedResultDF: DataFrame, primaryKey: String, checkingCol: String): Boolean =
      {
        val resultColName = "result" + checkingCol
        val storedResDF: DataFrame = storedResultDF
          .withColumnRenamed(checkingCol, resultColName)

        val joinedDF: DataFrame = computedDF.join(storedResDF, primaryKey)
        val errorsDF = joinedDF.filter(joinedDF(checkingCol) =!= joinedDF(resultColName))
        val errorsDFlen = errorsDF.count()

        if (errorsDFlen > 0) errorsDF.show()

        errorsDFlen == 0
      }


      withClue("stopwords Arrays match ")
      {
        assert(compareValues(resultsDF, solutionsKeyPair, primaryKey, "stopWordFiltered"))
      }


      withClue("regex replacement match ")
      {
        assert(compareValues(resultsDF, solutionsKeyPair, primaryKey, "RegexRepl"))
      }

      withClue("word class replacement match")
      {
        assert(compareValues(resultsDF, solutionsKeyPair, primaryKey, "wordClassRepl"))
      }

      withClue("contraction replacement match")
      {
        assert(compareValues(resultsDF, solutionsKeyPair, primaryKey, "contractionReplacement"))
      }

      withClue("regex removal of text data match ")
      {
        assert(compareValues(resultsDF, solutionsKeyPair, primaryKey, "regexRemovalText"))
      }

      withClue("stemming of text data match")
      {
        assert(compareValues(resultsDF, solutionsKeyPair, primaryKey, "stemmingText"))
      }

      withClue("sentence marker match")
      {
        assert(compareValues(resultsDF, solutionsKeyPair, primaryKey, "SentenceMarker"))
      }

      withClue("case normalization data match")
      {
        assert(compareValues(resultsDF, solutionsKeyPair, primaryKey, "caseNormalizationText"))
      }
    }

}
