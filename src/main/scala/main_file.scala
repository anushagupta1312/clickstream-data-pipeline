import constants.constants
import file_reader.File_Reader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object main_file {
  def main(args: Array[String]): Unit= {
    val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("InClassTasks")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )
    val clickstream_df = File_Reader.file_Reader(spark, constants.path_clickstream, "csv")
    //clickstream_df.show(30)

    clickstream_df.withColumn("id",col("id").cast("int"))
    clickstream_df.printSchema()
    val item_df = File_Reader.file_Reader(spark, constants.path_item, "csv")
    //item_df.show(30)
  }

}
