import constants.constants
import file_reader.file_Reader.file_Reader
import org.apache.spark.sql.SparkSession
import file_cleanser.modifying_datatypes.{changeDatatype, convertStoT}

object main_file {
  def main(args: Array[String]): Unit= {
    val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("InClassTasks")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    /************CLICKSTREAM DATASET***************/
    var clickstream_df = file_Reader(spark, constants.path_clickstream, "csv")
    clickstream_df = convertStoT(clickstream_df, "event_timestamp", "MM/dd/yyyy HH:mm")
    clickstream_df = changeDatatype(clickstream_df, constants.datatype_clickstream)

    //val distinctValueDf = clickstream_df.select(clickstream_df("is_add_to_cart")).distinct().show()

    /*************ITEM DATASET************/
    var item_df = file_Reader(spark, constants.path_item, "csv")
    item_df = changeDatatype(item_df, constants.datatype_item)
    
  }

}
