package writer

import org.apache.spark.sql.DataFrame

object FileWriter {

  //creates a file to store the data of a dataframe in a specified format
  def fileWriter (inputDF : DataFrame, filePath : String, fileFormat: String) : Unit = {
    inputDF.write.option("header", true).mode("overwrite").format(fileFormat).save(filePath)
  }

}
