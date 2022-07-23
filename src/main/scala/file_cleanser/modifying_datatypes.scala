package file_cleanser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_timestamp}

object modifying_datatypes {
  def convertStoT(df : DataFrame, col_name: String, format : String) : DataFrame = {
    val new_df = df.withColumn(col_name, to_timestamp(col(col_name),format))
    new_df
  }

  def changeDatatype(df : DataFrame, datatype_seq : Seq[(String,String)]) : DataFrame = {
    val new_df = df.select(datatype_seq.map{case(c,t) => df.col(c).cast(t)}:_*)
    new_df.printSchema()
    new_df.show()
    new_df
  }

}
