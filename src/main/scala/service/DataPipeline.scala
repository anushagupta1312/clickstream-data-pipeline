package service

import checks.DataQualityChecks.{ nullCheck, schemaValidationCheck}
import cleanser.FileCleanser._
import com.typesafe.config.Config
import constants.ApplicationConstants
import constants.ApplicationConstants.{CLICK_STREAM_DATATYPE, CLICK_STREAM_INPUT_PATH, CLICK_STREAM_OUTPUT_PATH, FILE_FORMAT, ITEM_DATATYPE, ITEM_DATA_INPUT_PATH, ITEM_OUTPUT_PATH}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import service.FileReader.fileReader
import service.FileWriter.writeToOutputPath
import utils.ApplicationUtils.{configuration, createSparkSession}

object DataPipeline {
  implicit val spark:SparkSession = createSparkSession()
  val appConf: Config = configuration()
  val log: Logger = Logger.getLogger(getClass)
  val clickStreamInputPath: String =appConf.getString(CLICK_STREAM_INPUT_PATH)
  val itemDataInputPath: String = appConf.getString(ITEM_DATA_INPUT_PATH)
  val clickStreamOutputPath: String=appConf.getString(CLICK_STREAM_OUTPUT_PATH)
  val itemDataOutputPath: String=appConf.getString(ITEM_OUTPUT_PATH)


  def execute(): Unit = {
    /*****************CLICK STREAM DATASET**********************/
    //reading click stream dataset
    val clickStreamDF = fileReader(clickStreamInputPath, FILE_FORMAT)

    //Removing rows when primary columns are null
    val rowEliminatedDF = removeRows(clickStreamDF, ApplicationConstants.CLICK_STREAM_NOT_NULL_KEYS)

    //Replacing null in other rows
    val numericFilledDF = fillCustomValues(rowEliminatedDF, ApplicationConstants.CLICK_STREAM_NUMERIC, "-1")
    val timestampFilledDF = fillCurrentTime(numericFilledDF, ApplicationConstants.CLICK_STREAM_TIMESTAMP)
    val falseFilledDF = fillCustomValues(timestampFilledDF, ApplicationConstants.CLICK_STREAM_BOOLEAN, "FALSE")
    val unknownFilledDF = fillCustomValues(falseFilledDF, ApplicationConstants.CLICK_STREAM_STRING, "UNKNOWN")

    //converting string to timestamp format
    val convertedDF = stringToTimestamp(unknownFilledDF, ApplicationConstants.TIME_STAMP_COL, ApplicationConstants.INPUT_TIME_STAMP_FORMAT)

    //converting redirection column into lowercase
    val modifiedDF = toLowercase(convertedDF, ApplicationConstants.REDIRECTION_COL)

    //modifying the data types of the columns of the click stream dataset
    val modifiedClickStreamDF = colDatatypeModifier(modifiedDF, ApplicationConstants.CLICK_STREAM_DATATYPE)

    //remove duplicates from the click stream dataset
    val clickStreamDFWithoutDuplicates = removeDuplicates(modifiedClickStreamDF, ApplicationConstants.CLICK_STREAM_PRIMARY_KEYS, Some(ApplicationConstants.TIME_STAMP_COL))

    //performing data quality checks on clickstream dataset
    val clickStreamMandatoryCol = CLICK_STREAM_DATATYPE.map(x => x._1)
    val itemMandatoryCol = ITEM_DATATYPE.map(x => x._1)
    nullCheck(clickStreamDFWithoutDuplicates, clickStreamMandatoryCol)
    schemaValidationCheck(clickStreamDFWithoutDuplicates)

    //logging information about click stream dataset
    log.warn("Total items in the click stream dataset " + clickStreamDFWithoutDuplicates.count())

    //writing the resultant data to a file
    writeToOutputPath(clickStreamDFWithoutDuplicates, clickStreamOutputPath, ApplicationConstants.FILE_FORMAT)

    /** **************ITEM DATASET*************** */
    //reading item dataset
    val itemDF = fileReader(itemDataInputPath, ApplicationConstants.FILE_FORMAT)

    //handling null values for item dataset
    val rowEliminatedItemDF = removeRows(itemDF, ApplicationConstants.ITEM_NOT_NULL_KEYS)

    //Replacing null in other rows
    val minusFilledItemDF = fillCustomValues(rowEliminatedItemDF, ApplicationConstants.ITEM_DATA_NUMERIC, "-1")
    val unknownFilledItemDF = fillCustomValues(minusFilledItemDF, ApplicationConstants.ITEM_DATA_STRING, "UNKNOWN")

    //modifying the data types of the columns of the item dataset
    val modifiedItemDF = colDatatypeModifier(unknownFilledItemDF, ApplicationConstants.ITEM_DATATYPE)

    //remove duplicates from the item dataset
    val itemDFWithoutDuplicates = removeDuplicates(modifiedItemDF, ApplicationConstants.ITEM_PRIMARY_KEYS, None)

    //logging information about item dataset
    log.warn("Total items in the item dataset " + itemDFWithoutDuplicates.count())

    //performing data quality checks on item dataset
    nullCheck(itemDFWithoutDuplicates, itemMandatoryCol)
    schemaValidationCheck(itemDFWithoutDuplicates)

    //writing the resultant data of item dataset to a file
    writeToOutputPath(itemDFWithoutDuplicates, itemDataOutputPath, ApplicationConstants.FILE_FORMAT)

  }

}
