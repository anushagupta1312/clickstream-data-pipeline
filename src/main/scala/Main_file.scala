import constants.constants
import file_reader.file_Reader.file_Reader
import org.apache.spark.sql.SparkSession
import file_cleanser.modifying_datatypes.{changeDatatype, convertStoT, toLowercase}
import org.apache.spark.sql.types.BooleanType

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
    val clickstream_df = file_Reader(spark, constants.path_clickstream, "csv")
    val clickstream_df_mod1 = convertStoT(clickstream_df, "event_timestamp", "MM/dd/yyyy HH:mm")
    val clickstream_df_mod2 = toLowercase(clickstream_df_mod1, "redirection_source")
    val clickstream_df_mod3 = changeDatatype(clickstream_df_mod2, constants.datatype_clickstream)

    //val distinctValueDf = clickstream_df.select(clickstream_df("is_add_to_cart")).distinct().show()

    /*************ITEM DATASET************/
    val item_df = file_Reader(spark, constants.path_item, "csv")
    val item_df_mod1 = changeDatatype(item_df, constants.datatype_item)

  }

}
