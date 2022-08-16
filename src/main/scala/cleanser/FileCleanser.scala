package cleanser

import utils.ApplicationUtils.check
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ col, desc, lower, row_number, to_timestamp}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object FileCleanser {

  /*********************REMOVING NULLS FROM THE DATASET****************************/
  //Handling null values - removing rows when primary key is null
  def removeRows(df: DataFrame, primaryColumns:Seq[String]): DataFrame = {
    val rowEliminatedDf = df.na.drop("any",primaryColumns)
    rowEliminatedDf
  }

  //Handling null values - filling null value with a custom value
  def fillCustomValues(df:DataFrame, columnsSeq:Seq[String], customVal:String):DataFrame = {
    val filledDf:DataFrame = df.na.fill(customVal,columnsSeq)
    filledDf
  }

  //Handling null values -filling null value with the current timestamp
  def fillCurrentTime(df:DataFrame, columnsSeq:Seq[String]):DataFrame = {
    val currentTime = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm").format(LocalDateTime.now)
    val timestampFilledDf:DataFrame = df.na.fill(currentTime:String,columnsSeq)
    timestampFilledDf
  }

  /**************MODIFYING COLUMN DATA TYPES*********************/
  //converts string to timestamp format
  def stringToTimestamp(inputDF : DataFrame, colName: String, inputFormat : String) : DataFrame = {
    check(inputDF, colName)
    val outputDF = inputDF.withColumn(colName, to_timestamp(col(colName),inputFormat))
    outputDF
  }

  //converts the string to lowercase
  def toLowercase(inputDF : DataFrame, colName: String) : DataFrame = {
    check(inputDF, colName)
    val outputDF = inputDF.withColumn(colName, lower(col(colName)))
    outputDF
  }

  //modifies the datatype of the columns in a dataframe
  def colDatatypeModifier(inputDF : DataFrame, colDatatype : List[(String, String)]) : DataFrame = {
    val colList = colDatatype.map(x => x._1)
    colList.foreach { (element: String) => check(inputDF, element) }
    //val dataTypeList = colDatatype.map(x => x._2)
    val outputDF = inputDF.select(colDatatype.map{x => inputDF.col(x._1).cast(x._2)}: _*)//_* ?
    outputDF
  }

  /******************REMOVING DUPLICATES FROM THE DATASET******************/
  //Handling Duplicates
  def removeDuplicates(df: DataFrame, primaryKeyCols : Seq[String], orderByCol: Option[String]) : DataFrame = {
    orderByCol match {
      case Some(column) =>
        //Remove duplicates from the click stream dataset
        val dfRemoveDuplicates = df.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col): _*).orderBy(desc(column))))
          .filter(col("rn") === 1).drop("rn")
        dfRemoveDuplicates
      //Remove duplicates from the item dataset
      case None => val dfRemoveDuplicates = df.dropDuplicates(primaryKeyCols)
        dfRemoveDuplicates
    }
  }

}