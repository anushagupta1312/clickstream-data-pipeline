package transform

import constants.ApplicationConstants.join_type
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp

class JoinTransform extends AnyFlatSpec {
  implicit val spark = utils.ApplicationUtils.createSparkSession()

  import spark.implicits._

  "joinDataFrame method()" should "do left join on two data set"  in {
    val clickstreamDF = Seq((
      30503, "2020-11-15 15:27:00", "android", "B000078", "I7099", "B741", "facebook", true, true),
      (30542, "2020-01-20 15:00:00", "android", "B000078", "I7099", "D8142", "google", true, true),
      (13931, "2020-11-15 09:07:00", "android", "B000092", "C2146", "H1406", "facebook", false, false),
      (13931, "2020-11-15 19:07:00", "android", "unknown", "C2146", "G6601", "facebook", false, false)
    ).toDF(
      "id",
      "event_timestamp",
      "device_type",
      "session_id",
      "visitor_id",
      "item_id",
      "redirection_source",
      "is_add_to_cart",
      "is_order_placed"
    )
    val joinedTableSchema = List(
      StructField("item_id", StringType, true),
      StructField("id", IntegerType, true),
      StructField("event_timestamp", TimestampType, true),
      StructField("device_type", StringType, true),
      StructField("session_id", StringType, true),
      StructField("visitor_id", StringType, true),
      StructField("redirection_source", StringType, true),
      StructField("is_add_to_cart", BooleanType, true),
      StructField("is_order_placed", BooleanType, true),
      StructField("item_price", DoubleType, true),
      StructField("product_type", StringType, true),
      StructField("department_name", StringType, true),
      StructField("vendor_id", IntegerType, true),
      StructField("vendor_name", StringType, true))

    //        ))
    val itemDF = Seq(
      ("B741", 192.2, "B003", "Furniture", 4, "LARVEL"),
      ("G6601", 92.5, "I116", "Clothing & Accessories", 1, "KOROL"),
      ("C1573", 33510.0, "F689", "Furniture", 1, "KOROL"),
      ("H1406", 1292.5, "C182", "Apps & Games", 3, "MOJO")).toDF("item_id", "item_price", "product_type", "department_name", "vendor_id", "vendor_name")

    //("D8142", "30542", "2020-01-20 15:00:00", "android", "B000078", "I7099", "google", true, true, null, null, null, null, null),


    val expectedDF = Seq(

      Row("B741", 30503, Timestamp.valueOf("2020-11-15 15:27:00"), "android", "B000078", "I7099", "facebook", true, true, (192.2), "B003", "Furniture", (4), "LARVEL"),
      Row("D8142", 30542, Timestamp.valueOf("2020-01-20 15:00:00"), "android", "B000078", "I7099", "google", true, true, null, null, null, null, null),
      Row("H1406", 13931, Timestamp.valueOf("2020-11-15 09:07:00"), "android", "B000092", "C2146", "facebook", false, false, (1292.5), "C182", "Apps & Games", (3), "MOJO"),
      Row("G6601", 13931, Timestamp.valueOf("2020-11-15 19:07:00"), "android", "unknown", "C2146", "facebook", false, false, (92.5), "I116", "Clothing & Accessories", (1), "KOROL")
    )
    ///
    val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedDF), StructType(joinedTableSchema))
    val outputDf: DataFrame = transform.JoinDatasets.joinDataFrame(clickstreamDF, itemDF, constants.ApplicationConstants.join_key, join_type)
    df.show()
    outputDf.show()


    val result = df.except(outputDf)
    val ans = result.count()
    val count = 0;
    assertResult(count)(ans)

  }

}