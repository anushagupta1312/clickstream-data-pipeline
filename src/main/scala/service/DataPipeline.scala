package service

import checks.DataQualityChecks._
import cleanser.FileCleanser._
import com.typesafe.config.Config
import constants.ApplicationConstants._
import org.apache.spark.sql.{DataFrame, SparkSession}
import service.FileReader.fileReader
import service.FileWriter.fileWriter
import transform.JoinDatasets._

object DataPipeline {
  //performs initial steps for click stream dataset
  def clickStreamInitialSteps(filePath: String, fileFormat: String, database_URL : String)(implicit sparkSession: SparkSession): DataFrame = {
    //reads the file into a dataframe
    val initialDF = fileReader(filePath, fileFormat)

    //modifying column dataTypes
    val changeDataTypeDF = stringToTimestamp(initialDF, TIME_STAMP_COL, INPUT_TIME_STAMP_FORMAT)

    //eliminate rows on NOT_NULL_COLUMNS
    val rowEliminatedDF = removeRows(changeDataTypeDF, CLICK_STREAM_PRIMARY_KEYS, database_URL, NULL_TABLE_CLICK_STREAM)

    // fill null values
    val nullFilledDF = fillValues(rowEliminatedDF, COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP)

    //converting redirection column into lowercase
    val modifiedDF = toLowercase(nullFilledDF, REDIRECTION_COL)

    //remove duplicates from the dataset
    val DFDeDuplicates = removeDuplicates(database_URL, modifiedDF, CLICK_STREAM_PRIMARY_KEYS, Some(TIME_STAMP_COL))

    DFDeDuplicates
  }

  //performs initial steps for item dataset
  def itemInitialSteps( filePath: String, fileFormat: String, database_URL : String)(implicit sparkSession: SparkSession): DataFrame = {
    //reads the file into a dataframe
    val initialDF = fileReader(filePath, fileFormat)

    //eliminate rows on NOT_NULL_COLUMNS
    val rowEliminatedDF = removeRows(initialDF, ITEM_PRIMARY_KEYS, database_URL, NULL_TABLE_ITEM)

    //fill null values
    val nullFilledDF = fillValues(rowEliminatedDF, COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)

    //remove duplicates from the dataset
    val DFDeDuplicates = removeDuplicates(database_URL, nullFilledDF, ITEM_PRIMARY_KEYS, None)

    DFDeDuplicates
  }

  //executes the pipeline by joining the two datasets and loading it into mysql database
  def execute(appConf: Config)(implicit sparkSession: SparkSession): Unit = {

    //inputting data from conf file
    val clickStreamInputPath: String = appConf.getString(CLICK_STREAM_INPUT_PATH)
    val itemDataInputPath: String = appConf.getString(ITEM_DATA_INPUT_PATH)
    val database_URL = appConf.getString(DATABASE_URL)
    val schemaPath = appConf.getString(SCHEMA_PATH)

    //clickStreamInitialSteps returns a dataframe without duplicates
    val clickStreamDFDeDuplicates = clickStreamInitialSteps(clickStreamInputPath, FILE_FORMAT, database_URL)

    //itemInitialSteps returns a dataframe without duplicates
    val itemDFDeDuplicates = itemInitialSteps(itemDataInputPath, FILE_FORMAT, database_URL)

    //  joining two datasets
    val joinedDataframe = joinDataFrame(clickStreamDFDeDuplicates, itemDFDeDuplicates, JOIN_KEY, JOIN_TYPE)

    //handling null values for joined dataframe
    val nullHandledJoinTable = fillValues(joinedDataframe, COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP)

    // transform operation is performed
    val enrichedJoinedDF = enrichDataFrame(nullHandledJoinTable)

    //performing data quality checks on the final dataframe before loading it into mysql database
    val correctSchemaDF = schemaValidationCheck(enrichedJoinedDF, schemaPath)
    nullCheck(correctSchemaDF, FINAL_TABLE_COL)
    duplicatesCheck(correctSchemaDF, FINAL_PRIMARY_KEY, TIME_STAMP_COL)

    //final df to be inserted - write into table
    fileWriter(database_URL, TABLE_NAME, correctSchemaDF)

  }
}
