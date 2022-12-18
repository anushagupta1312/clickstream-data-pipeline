package service

import exceptions.Exceptions.{DataframeIsEmptyException, EmptyFilePathException, FilePathNotFoundException, FileReaderException, InvalidInputFormatException}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

object FileReader {

  //used to read a file from a specified location into a dataframe
  def fileReader(filePath: String, fileFormat: String)(implicit sparkSession: SparkSession): DataFrame = {

    //if input path is empty
    if (filePath == "") {
      throw EmptyFilePathException("The file path is empty. Please provide a valid file path.")
    }

    //reads the file into a dataframe
    val outputDF = try {
      sparkSession.read.option("header", "true").option("inferschema", value = true).format(fileFormat).load(filePath)

    } catch {
      case _: AnalysisException => throw FilePathNotFoundException("The file path is not found.")
      case _: java.lang.ClassNotFoundException => throw InvalidInputFormatException("The file format is invalid")
      case _: Exception => throw FileReaderException("Unable to read file from given path.")
    }

    //if dataframe does not contain any records
    if (outputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataFrame is empty")
    }
    outputDF
  }
}
