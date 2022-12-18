package checks

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.ApplicationUtils.schemaRead
import exceptions.Exceptions.{NullValuesExistException, DuplicateValuesExistException}

object DataQualityChecks {

  //null data quality check
  def nullCheck(inputDF: DataFrame, columns: List[String])(implicit spark: SparkSession): Unit = {
    columns.foreach(c => {
      if(inputDF.filter(inputDF(c).isNull
        || inputDF(c) === ""
        || inputDF(c).contains("NULL")
        || inputDF(c).contains("null")).count() != 0){
          throw NullValuesExistException("Null values are present in the dataset")
      }
    })
  }

  //duplicates data quality check
  def duplicatesCheck(inputDF: DataFrame, primaryKeyCols: Seq[String], orderByCol: String): Unit = {
    val exceptionsDF = inputDF.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col): _*).orderBy(desc(orderByCol))))
      .filter(col("rn") >1).drop("rn")
    if(exceptionsDF.count() > 0){
      throw DuplicateValuesExistException("Duplicates found in click stream dataset")
    }
  }

  //schema validation check
  def schemaValidationCheck(inputDF: DataFrame, schemaPath: String)(implicit sparkSession: SparkSession): DataFrame = {
    val dfSchema = inputDF.schema
    val correctSchema = schemaRead(schemaPath)
    if(dfSchema != correctSchema){
      val outputDF = sparkSession.createDataFrame(inputDF.rdd, correctSchema)
      outputDF
    }else{
      inputDF
    }
  }
}
