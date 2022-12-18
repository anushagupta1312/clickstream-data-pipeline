package service

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import service.FileReader.fileReader

import java.sql.Timestamp

class FileReaderTest extends AnyFlatSpec {

  implicit val spark: SparkSession = helper.Helper.createSparkSession()
  "file reader " should "return a return a dataframe with records " in {

    val outputDF = fileReader(helper.Helper.CLICK_STREAM_TEST_INPUT_PATH, helper.Helper.INPUT_TEST_FILE_FORMAT)
    val expectedRDD = Seq(
      Row(29839, null, "android", "B000078", "I7099", "B17543", "GOOGLE", null, true),
      Row(30504, "11/15/2020 15:27", "android", "B000078", "I7099", "B19304", "LinkedIn", null, true),
      Row(30334, "11/15/2020 15:23", "android", "B000078", "I7099", "B29093", "Youtube", null, null),
      Row(30385, "11/15/2020 15:24", "android", "B000078", "I7099", "D8142", "google", true, null))

    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedRDD), helper.Helper.CLICK_STREAM_READER_SCHEMA)
    val result = expectedDF.except(outputDF)
    val ans = result.count()
    val count = 0
    assertResult(count)(ans)
  }

}

