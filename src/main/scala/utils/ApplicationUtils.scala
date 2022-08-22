package utils

import com.typesafe.config.{Config, ConfigFactory}
import exceptions.Exceptions.{ColumnNotFoundException, DataframeIsEmptyException}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object ApplicationUtils {

  //configuration
  def configuration():Config = {
    val parsedConfig = ConfigFactory.parseFile(new File("conf/local.conf"))
    val appConf: Config = ConfigFactory.load(parsedConfig)
    //    val appName = appConf.getString("spark.app.name")
    //    val appMaster = appConf.getString("spark.app.master")
    appConf
  }

  //Creating the spark session
  def createSparkSession(): SparkSession = {
    implicit val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName(configuration().getString("spark.app.name"))
        .master(configuration().getString("spark.app.master"))
        .enableHiveSupport()
        .getOrCreate()
    )
    spark
  }

  //checking for exceptions
  def check(inputDF : DataFrame, colName :String): Unit = {
    if(inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    } else if (!inputDF.columns.contains(colName))
      throw ColumnNotFoundException("The specified column does not exist")
  }

}