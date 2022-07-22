package file_reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object File_Reader {
  def file_Reader(spark:SparkSession, path:String, fileType:String): DataFrame = {
    val df = spark.read.option("header", "true").format(fileType).load(path)
    df
  }

}
