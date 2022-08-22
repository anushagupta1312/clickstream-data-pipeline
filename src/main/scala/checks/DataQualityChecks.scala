package checks

import exceptions.Exceptions.{DuplicateValuesExistException, NullValuesExistException, SchemaValidationFailedException}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.ApplicationUtils.createSparkSession
import scala.collection.JavaConversions._

object DataQualityChecks {

  implicit val spark: SparkSession = createSparkSession()

  var count = 0
  var errorList: List[Row] = List[Row]()
  val errorSchema: StructType = StructType(Array(
    StructField("item_id", StringType, nullable = true),
    StructField("id", IntegerType,nullable = true),
    StructField("event_timestamp", TimestampType,nullable = true),
    StructField("device_type", StringType,nullable = true),
    StructField("session_id", StringType,nullable = true),
    StructField("visitor_id", StringType,nullable = true),
    StructField("redirection_source",StringType,nullable = true),
    StructField("is_add_to_cart", BooleanType,nullable = true),
    StructField("is_order_placed", BooleanType,nullable = true),
    StructField("item_price",DoubleType,nullable = true),
    StructField("product_type",StringType,nullable = true),
    StructField("department_name",StringType,nullable = true),
    StructField("vendor_id", IntegerType,nullable = true),
    StructField("vendor_name",StringType,nullable = true),
    StructField("event_d",DateType,nullable = true),
    StructField("record_load_ts",TimestampType,nullable = true)))

  //nulls
  def nullCheck(inputDF: DataFrame, columns: List[String]): Unit = {
//    columns.foreach(c => {
//      if(inputDF.filter(inputDF(c).isNull
//        || inputDF(c) === ""
//        || inputDF(c).contains("NULL")
//        || inputDF(c).contains("null")).count() != 0){
//        throw NullValuesExistException("Null values are present in the dataset")
//      }
//    })
    var count = 0
    var errorList = List[Row]()
    inputDF.collect().foreach(row => {
      row.toSeq.foreach(c => {
        if (c == "UNKNOWN" || c == -1 || c == false) {
          count = count + 1
        }
      })
      if (count > 8) {
        errorList = errorList :+ row
      }
      count = 0
    })

    val errorDF = spark.createDataFrame(errorList, errorSchema)
    errorDF.show()
  }

  //duplicates check
  def duplicatesCheck(inputDF: DataFrame, primaryKeyCols : Seq[String], orderByCol: Option[String]) : Unit = {
    orderByCol match {
      case Some(column) =>
        val exceptionsDF = inputDF.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col): _*).orderBy(desc(column))))
          .filter(col("rn") >1).drop("rn")
        if(exceptionsDF.count() != 0){
          throw DuplicateValuesExistException("Duplicates found in click stream dataset")
        }
      case None =>
        if(inputDF.except(inputDF.dropDuplicates(primaryKeyCols)).count() != 0){
          throw DuplicateValuesExistException("Duplicates found in item dataset")
        }
    }
  }

  //schema validation
  def schemaValidationCheck(inputDF : DataFrame): Unit ={
    inputDF.schema.fields.foreach(f=> {
      inputDF.select(f.name).foreach(c => {
        if(!c(0).getClass.getName.toLowerCase.contains(f.dataType.toString.toLowerCase().split("type")(0))){
          throw SchemaValidationFailedException(c(0) + " does not have the same datatype as its column " + f.name)
        }
      })
    })
  }

  //categorical

}
