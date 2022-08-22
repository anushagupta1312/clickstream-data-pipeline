package cleanser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import utils.ApplicationUtils.check
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object FileCleanser {

  /** *******************REMOVING NULLS FROM THE DATASET*************************** */
  //Handling null values - removing rows when primary key is null
  def removeRows(df: DataFrame, primaryColumns: Seq[String]): DataFrame = {
    val rowEliminatedDf = df.na.drop("any", primaryColumns)
    rowEliminatedDf
  }

  //Handling null values - filling null value with a custom value
  def fillValues(df: DataFrame, nullMap: Map[String, Any]): DataFrame = {
    val filledDf: DataFrame = df.na.fill(nullMap)
    filledDf
  }

  //Handling null values -filling null value with the current timestamp
  def fillCurrentTime(inputDF: DataFrame): DataFrame = {
    val currentTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())
    val fillTimeDF = inputDF.withColumn("event_timestamp", coalesce(col("event_timestamp"), lit(currentTime).cast(TimestampType)))
    fillTimeDF
  }

  /** ************MODIFYING COLUMN DATA TYPES******************** */
  //converts string to timestamp format
  def stringToTimestamp(inputDF: DataFrame, colName: String, inputFormat: String): DataFrame = {
    check(inputDF, colName)
    val outputDF = inputDF.withColumn(colName, to_timestamp(col(colName), inputFormat))
    outputDF
  }

  //converts the string to lowercase
  def toLowercase(inputDF: DataFrame, colName: String): DataFrame = {
    check(inputDF, colName)
    val outputDF = inputDF.withColumn(colName, lower(col(colName)))
    outputDF
  }

  //modifies the datatype of the columns in a dataframe
  def colDatatypeModifier(inputDF: DataFrame, colDatatype: List[(String, String)]): DataFrame = {
    val colList = colDatatype.map(x => x._1)
    colList.foreach { (element: String) => check(inputDF, element) }
    //val dataTypeList = colDatatype.map(x => x._2)
    //    val outputDF = inputDF.select(colDatatype.map{case(c,t) => inputDF.col(c).cast(t)}:_*)
    val outputDF = inputDF.select(colDatatype.map { x => inputDF.col(x._1).cast(x._2) }: _*)
    outputDF
  }

  /** ****************REMOVING DUPLICATES FROM THE DATASET***************** */
  //Handling Duplicates
  def removeDuplicates(df: DataFrame, primaryKeyCols: Seq[String], orderByCol: Option[String] = None): DataFrame = {

    orderByCol match {
      case Some(column) =>
        //Remove duplicates from the click stream dataset
        val dfRemoveDuplicates = df.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col): _*).orderBy(desc(column))))
          .filter(col("rn") === 1).drop("rn")
        dfRemoveDuplicates
      //Remove duplicates from the item dataset
      case None =>
        val dfRemoveDuplicates = df.dropDuplicates(primaryKeyCols)
        dfRemoveDuplicates
    }
  }
}