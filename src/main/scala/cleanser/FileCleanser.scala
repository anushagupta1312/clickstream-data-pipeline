package cleanser

import constants.ApplicationConstants.{ERR_TABLE_DUP_CLICK_STREAM, ERR_TABLE_DUP_ITEM}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import service.FileWriter
import utils.ApplicationUtils.check

import scala.collection.JavaConversions._

object FileCleanser {

  /** *******************REMOVING NULLS FROM THE DATASET*************************** */
  //Handling null values - removing rows when primary key is null
  def removeRows(inputDF: DataFrame, primaryColumns: Seq[String], databaseUrl: String, tableName: String)(implicit spark: SparkSession): DataFrame = {
    check(inputDF, primaryColumns)
    val rowEliminatedDF = inputDF.na.drop("any", primaryColumns)
    var count = 0
    var errorList: List[Row] = List[Row]()
    rowEliminatedDF.collect().foreach(row => {
      row.toSeq.foreach(c => {
        if (c == "null" || c == "NULL" || c == "" || c == null) {
          count = count + 1
        }
      })
      if (count > row.length * 0.6) {
        errorList = errorList :+ row
      }
      count = 0
    })
    val errorDF = spark.createDataFrame(errorList, inputDF.schema)
    FileWriter.fileWriter(databaseUrl, tableName, errorDF)
    val nullsRemDF = rowEliminatedDF.except(errorDF)
    nullsRemDF
  }

  //Handling null values - filling null value with a custom value
  def fillValues(inputDF: DataFrame, nullMap: Map[String, Any]): DataFrame = {
    val colList = nullMap.keys.toSeq
    check(inputDF, colList)
    val filledDf: DataFrame = inputDF.na.fill(nullMap)
    filledDf
  }

  /** ************MODIFYING COLUMN DATA TYPES******************** */
  //converts string to timestamp format
  def stringToTimestamp(inputDF: DataFrame, colName: String, inputFormat: String): DataFrame = {
    check(inputDF, Seq(colName))
    val outputDF = inputDF.withColumn(colName, to_timestamp(col(colName), inputFormat))
    outputDF
  }

  //converts the string to lowercase
  def toLowercase(inputDF: DataFrame, colName: String): DataFrame = {
    check(inputDF, Seq(colName))
    val outputDF = inputDF.withColumn(colName, lower(col(colName)))
    outputDF
  }

  /** ****************REMOVING DUPLICATES FROM THE DATASET***************** */
  //Handling Duplicates
  def removeDuplicates(databaseUrl:String, inputDF: DataFrame, primaryKeyCols: Seq[String], orderByCol: Option[String]): DataFrame = {
    check(inputDF, primaryKeyCols)
    
    orderByCol match {
      case Some(column) =>
        //Remove duplicates from the click stream dataset
        val dfRemoveDuplicates = inputDF.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col): _*).orderBy(desc(column))))
          .filter(col("rn") === 1).drop("rn")
        //Put duplicate records to the duplicate error table
        val errorDuplicateDF = inputDF.except(dfRemoveDuplicates)
        FileWriter.fileWriter(databaseUrl,ERR_TABLE_DUP_CLICK_STREAM, errorDuplicateDF)
        dfRemoveDuplicates
      
        //Remove duplicates from the item dataset
      case None =>
        val dfRemoveDuplicates = inputDF.dropDuplicates(primaryKeyCols)
        //Put duplicate records to the duplicate error table
        val errorDuplicateDF = inputDF.except(dfRemoveDuplicates)
        FileWriter.fileWriter(databaseUrl,ERR_TABLE_DUP_ITEM, errorDuplicateDF)
        dfRemoveDuplicates
    }
  }
}
