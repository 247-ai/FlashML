package com.tfs.flashml.functionalTests

import com.tfs.flashml.core.PipelineSteps
import com.tfs.flashml.dal.{DataReader, SavePointManager}
import com.tfs.flashml.util.FlashMLConfig
import com.tfs.flashml.util.conf.FlashMLConstants
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

class DataReaderTest extends AnyFlatSpec
{
    private val log = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("=============================================================================================")
    log.info("Starting FlashML test application")
    println("Test case: Data Input and SQL execution")

    "Random variable from UUID generation" should "work" in
    {
        val ss = SparkSession
                .builder()
                .appName("RandValGenFromUUID")
                .config("spark.master", "local")
                .getOrCreate()

        import ss.implicits._

        /**
          * Class to test DataReader methods
          * @param df
          */
        class DataFrameReader(df: DataFrame) extends DataReader
        {
            override def read: DataFrame = df
        }

        val dataDF = Seq(
            ("00198e6f-2f38-47c4-94ac-66741d844ced", "Windows-XP", 0, 2.884),
            ("001b806f-631a-4a6d-81a5-dcea84104bd1", "MacOS", 1, 1.554),
            ("002542ec-5027-4b33-8490-b6884c22b554", "MacOS", 1, 1.889),
            ("0025ba19-ef1e-41dd-b0e9-cb4d3c25e978", "Android", 0, 2.687),
            ("05ba9ab5-a3b0-4048-8644-ef3fa7fc7d45", "Windows", 0, 1.667),
            ("078d6297-1084-4925-9919-500e03342081", "MacOS", 1, 2.334)
        ).toDF("vid", "device", "isFirstTime", "anotherval")

        val dfreader = new DataFrameReader(dataDF)
        FlashMLConfig.addToConfig(Map(
            FlashMLConstants.SAMPLING_TYPE -> FlashMLConstants.SAMPLING_TYPE_CONDITIONAL,
            FlashMLConstants.RANDOM_NUMBER_GENEARATOR_VARIABLE -> "vid"
        ))
        val expectedRands = Seq(
            ("00198e6f-2f38-47c4-94ac-66741d844ced", 0.03899594304128456),
            ("001b806f-631a-4a6d-81a5-dcea84104bd1", 0.041964254351790124),
            ("002542ec-5027-4b33-8490-b6884c22b554", 0.05685640500319572),
            ("0025ba19-ef1e-41dd-b0e9-cb4d3c25e978", 0.05756674801408471),
            ("05ba9ab5-a3b0-4048-8644-ef3fa7fc7d45", 2.2378606028477335),
            ("078d6297-1084-4925-9919-500e03342081", 2.9501115677296443)
        ).toDF("vid", "expectedRands")
        val computedRands = dfreader.generateRandomVariable(dataDF)
        assertResult(0)
        {
            computedRands.join(expectedRands, computedRands("vid") === expectedRands("vid"))
                    .withColumn("diff", computedRands(FlashMLConstants.RANDOM_VARIABLE_COLUMN_NAME) - expectedRands("expectedRands"))
                    .filter($"diff" > 1E-10d)
                    .count()
        }
        // We need to stop the SparkContext so as to maintain two independent tests.
        ss.stop()
    }

    "Original data and the re-loaded data" should "match" in
    {
        FlashMLConfig.config = ConfigFactory.load("data_reader_config.json")

        val appName = s"${FlashMLConfig.getString(FlashMLConstants.FLASHML_PROJECT_ID)}/${FlashMLConfig.getString(FlashMLConstants.FLASHML_MODEL_ID)}/${FlashMLConfig.getString(FlashMLConstants.FLASHML_JOB_ID)}"
        val context = FlashMLConfig.getString(FlashMLConstants.CONTEXT)
        val HIVE_METASTORE_KEY = "hive.metastore.uris"
        val HIVE_METASTORE_THRIFT_URL = FlashMLConfig.getString(FlashMLConstants.HIVE_THRIFT_URL)

        val ss = SparkSession.builder()
                .config(HIVE_METASTORE_KEY, HIVE_METASTORE_THRIFT_URL)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", "com.tfs.flashml.util.FlashMLKryoRegistrator")
                .config("spark.extraListeners", "com.tfs.flashml.util.CustomSparkListener")
                .config("spark.kryoserializer.buffer.max", "256")
                .config("spark.sql.parquet.compression.codec", "gzip")
                .config("spark.ui.showConsoleProgress", "False")
                .master(context)
                .appName(appName)
                .enableHiveSupport().getOrCreate()
        PipelineSteps.run()

        assertResult(0)
        {
            val inputData: DataFrame = SavePointManager.loadInputData
            inputData.createOrReplaceTempView("tmp_table")
            // match values of maxp with rolled up purchase flag
            val query = "select count(*) as count from " +
                    "(select a.*, b.purchase_flag_max from tmp_table a " +
                    "inner join (select max(purchase_flag) as purchase_flag_max, vid, active_session, dt " +
                    "from tmp_table " +
                    "group by vid, active_session, dt) b" +
                    " on a.vid = b.vid and a.active_session = b.active_session and a.dt = b.dt" +
                    " ) x where maxp != purchase_flag_max"
            val res = SparkSession.builder().getOrCreate().sql(query).head(1).map(row => row.getLong(0))
            res(0)
        }
        ss.stop()
    }

}
