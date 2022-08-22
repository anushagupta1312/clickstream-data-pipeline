package service

import org.scalatest.flatspec.AnyFlatSpec
import service.FileWriter.{decryptPassword, fileWriter}

class FileWriterTest extends AnyFlatSpec{

  implicit val spark = utils.ApplicationUtils.createSparkSession()

  "file writer " should "write a dataframe to a table " in {
    val inputDF= service.FileReader.fileReader("data/clickstream_log.csv", "csv")
    val tablename = "unittest"
    service.FileWriter. fileWriter(tablename, inputDF)
  }

}