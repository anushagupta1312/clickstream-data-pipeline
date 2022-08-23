package service

import checks.DataQualityChecks.{duplicatesCheck, nullCheck, schemaValidationCheck}
import cleanser.FileCleanser._
import com.typesafe.config.Config
import constants.ApplicationConstants._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import service.FileReader.fileReader
import service.FileWriter.fileWriter
import transform.JoinDatasets.joinDataFrame
import utils.ApplicationUtils.{configuration, createSparkSession}

object DataPipeline {
  implicit val spark: SparkSession = createSparkSession()
  val appConf: Config = configuration()
  val log: Logger = Logger.getLogger(getClass)
  val clickStreamInputPath: String = appConf.getString(CLICK_STREAM_INPUT_PATH)
  val itemDataInputPath: String = appConf.getString(ITEM_DATA_INPUT_PATH)
  val clickStreamOutputPath: String = appConf.getString(CLICK_STREAM_OUTPUT_PATH)
  val itemDataOutputPath: String = appConf.getString(ITEM_OUTPUT_PATH)
  val databaseURL: String = appConf.getString(DATABASE_URL)

  def initialSteps(filePath : String, fileFormat : String, timeStampCol: Option[String]): DataFrame = {
    val initialDF = fileReader(filePath, fileFormat)
    timeStampCol match {
      case Some(column) =>
        //modifying columns datatype
        val timeStampDataTypeDF = stringToTimestamp(initialDF, column, INPUT_TIME_STAMP_FORMAT)
        val changeDataTypeDF=colDatatypeModifier(timeStampDataTypeDF,CLICK_STREAM_DATATYPE)

        //eliminate rows on NOT_NULL_COLUMNS
        val rowEliminatedDF = removeRows(changeDataTypeDF,CLICK_STREAM_NOT_NULL_KEYS)

        // fill time stamp
        val timeFilledDF=fillCurrentTime(rowEliminatedDF)

        // fill null values
        val nullFilledDF=fillValues(timeFilledDF,COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)

        //converting redirection column into lowercase
        val modifiedDF = toLowercase(nullFilledDF, REDIRECTION_COL)

        //remove duplicates from the dataset
        val DFWithoutDuplicates = removeDuplicates(modifiedDF, CLICK_STREAM_PRIMARY_KEYS, Some(TIME_STAMP_COL))

        DFWithoutDuplicates

      case None =>
        val changeDataTypeDF=colDatatypeModifier(initialDF,ITEM_DATATYPE)

        //eliminate rows on NOT_NULL_COLUMNS
        val rowEliminatedDF = removeRows(changeDataTypeDF,ITEM_NOT_NULL_KEYS)

        //fill null values
        val nullFilledDF=fillValues(rowEliminatedDF,COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)

        //remove duplicates from the dataset
        val DFWithoutDuplicates = removeDuplicates(nullFilledDF, ITEM_PRIMARY_KEYS, None)

        DFWithoutDuplicates
    }
  }

  def execute(): Unit = {
    val clickStreamDFDeDuplicates = initialSteps(clickStreamInputPath, FILE_FORMAT, Some(TIME_STAMP_COL))

    val itemDFDeDuplicates = initialSteps(itemDataInputPath, FILE_FORMAT, None)

    //  joining two datasets
    val joinedDataframe = joinDataFrame(clickStreamDFDeDuplicates, itemDFDeDuplicates, join_key, join_type)

    val nullHandledJoinTable = fillValues(joinedDataframe,COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)

    // transform
    val transformJoinedDF = transform.JoinDatasets.transformDataFrame(nullHandledJoinTable)
    transformJoinedDF.show()

    //performing data quality checks on click stream dataset
    schemaValidationCheck(transformJoinedDF)
    val nullCheckFinalDF = nullCheck(transformJoinedDF)
    val duplicateCheckFinalDF = duplicatesCheck(nullCheckFinalDF, CLICK_STREAM_PRIMARY_KEYS, TIME_STAMP_COL)

    //final df to be inserted - write into table
    fileWriter(databaseURL, "table_try_6", duplicateCheckFinalDF)

  }
}
