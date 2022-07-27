import appConstants.Constants
import reader.FileReader.file_Reader
import org.apache.spark.sql.SparkSession
import cleanser.FileCleanser.{colDatatypeModifier, stringToTimestamp, toLowercase}
import org.apache.log4j.Logger

object DataPipeline {
  val log = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit= {

    //Creating the spark session
    val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("DataPipeline")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    /************CLICKSTREAM DATASET***************/

    //reading clickstream dataset
    val clickstreamDF = file_Reader(spark, Constants.CLICKSTREAM_PATH, "csv")

    //converting string to timestamp format
    val convertedDF = stringToTimestamp(clickstreamDF, "event_timestamp", "MM/dd/yyyy HH:mm")

    //converting redirection column into lowercase
    val modifiedDF = toLowercase(convertedDF, "redirection_source")

    //modifying the datatypes of the columns of the clickstream dataset
    val modifiedClickstreamDF = colDatatypeModifier(modifiedDF, Constants.CLICKSTREAM_DATATYPE)

    /****************ITEM DATASET****************/

    //reading item dataset
    val itemDF = file_Reader(spark, Constants.ITEM_DATA_PATH, "csv")

    //modifying the datatypes of the columns of the item dataset
    val modifiedItemDF = colDatatypeModifier(itemDF, Constants.ITEM_DATAYPE)

    //logging information about clickstream dataset
    log.info("Total items in the clickstream dataset " + modifiedClickstreamDF.count())
    log.info(modifiedClickstreamDF)

    //logging information about item dataset
    log.info("Total items in the item dataset " + modifiedItemDF.count())
    log.info(modifiedItemDF)

    log.warn(modifiedClickstreamDF.rdd.getNumPartitions)
    log.warn(modifiedItemDF.rdd.getNumPartitions)

  }

}

