package checks

import checks.DataQualityChecks.nullCheck
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import java.sql.Date

class DataQualityTest extends AnyFlatSpec{
  implicit val spark = utils.ApplicationUtils.createSparkSession()
  //import spark.implicits._

  "nullCheck " should " remove records having more than 60% of null values" in {

    val joinedTableSchema = List(
      StructField("item_id", StringType, nullable = true),
      StructField("id", IntegerType, nullable = true),
      StructField("event_timestamp", TimestampType, nullable = true),
      StructField("device_type", StringType, nullable = true),
      StructField("session_id", StringType, nullable = true),
      StructField("visitor_id", StringType, nullable = true),
      StructField("redirection_source", StringType, nullable = true),
      StructField("is_add_to_cart", BooleanType, nullable = true),
      StructField("is_order_placed", BooleanType, nullable = true),
      StructField("item_price", DoubleType, nullable = true),
      StructField("product_type", StringType, nullable = true),
      StructField("department_name", StringType, nullable = true),
      StructField("vendor_id", IntegerType, nullable = true),
      StructField("vendor_name", StringType, nullable = true),
      StructField("event_d",DateType,nullable = true),
      StructField("record_load_ts",TimestampType,nullable = true))

    val sampleDF = Seq(
      Row("B741", 30503, Timestamp.valueOf("2020-11-15 15:27:00"), "android", "B000078", "I7099", "facebook", true, true, (192.2), "B003", "Furniture", (4), "LARVEL", Date.valueOf("2020-11-15"), Timestamp.valueOf("2020-11-15 15:27:00")),
      Row("D8142", 30542, Timestamp.valueOf("2020-01-20 15:00:00"), "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", false, true, null, null, null, null, null, Date.valueOf("2020-11-15"), Timestamp.valueOf("2020-11-15 15:27:00")),
      Row("H1406", 13931, Timestamp.valueOf("2020-11-15 09:07:00"), "android", "B000092", "C2146", "facebook", false, false, (1292.5), "C182", "Apps & Games", (3), "MOJO", Date.valueOf("2020-11-15"), Timestamp.valueOf("2020-11-15 15:27:00")),
      Row("G6601", 13931, Timestamp.valueOf("2020-11-15 19:07:00"), "android", "unknown", "C2146", "facebook", false, false, (92.5), "I116", "Clothing & Accessories", (1), "KOROL", Date.valueOf("2020-11-15"), Timestamp.valueOf("2020-11-15 15:27:00"))
    )
    val inputDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(sampleDF), StructType(joinedTableSchema))

    val outputDF = nullCheck(inputDF)

    val expectedSampleDF = Seq(
      Row("B741", 30503, Timestamp.valueOf("2020-11-15 15:27:00"), "android", "B000078", "I7099", "facebook", true, true, (192.2), "B003", "Furniture", (4), "LARVEL", Date.valueOf("2020-11-15"), Timestamp.valueOf("2020-11-15 15:27:00")),
      Row("H1406", 13931, Timestamp.valueOf("2020-11-15 09:07:00"), "android", "B000092", "C2146", "facebook", false, false, (1292.5), "C182", "Apps & Games", (3), "MOJO", Date.valueOf("2020-11-15"), Timestamp.valueOf("2020-11-15 15:27:00")),
      Row("G6601", 13931, Timestamp.valueOf("2020-11-15 19:07:00"), "android", "unknown", "C2146", "facebook", false, false, (92.5), "I116", "Clothing & Accessories", (1), "KOROL", Date.valueOf("2020-11-15"), Timestamp.valueOf("2020-11-15 15:27:00"))
    )
    val expectedOutputDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedSampleDF), StructType(joinedTableSchema))

    outputDF.show()
    expectedOutputDF.show()

    val resultDF = outputDF.except(expectedOutputDF)
    val resultCount = resultDF.count()
    val count = 0
    assertResult(count)(resultCount)
  }

}
