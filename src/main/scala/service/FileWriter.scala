package service

import exceptions.Exceptions.{DataframeIsEmptyException, FileWriterException}
import org.apache.spark.sql.DataFrame

object FileWriter {

  //creates a file to store the data of a dataframe in a specified format
  def writeToOutputPath(inputDF: DataFrame, filePath: String, fileFormat: String): Unit = {

    if (inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    }
    try {
      inputDF.repartition(1).write.format(fileFormat).option("header", "true").mode("overwrite").save(filePath)
    } catch {
      case e: Exception => throw FileWriterException("Error in writing file to given location")
    }
  }
}
