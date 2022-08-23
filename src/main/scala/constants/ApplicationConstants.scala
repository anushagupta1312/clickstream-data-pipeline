package constants

object ApplicationConstants {
  //input path
  val CLICK_STREAM_INPUT_PATH: String = "spark.app.clickStreamInputPath"
  val ITEM_DATA_INPUT_PATH: String = "spark.app.itemDataInputPath"

  val CLICK_STREAM_PRIMARY_KEYS: Seq[String] = Seq("session_id", "visitor_id", "item_id")
  val ITEM_PRIMARY_KEYS: Seq[String] = Seq("item_id")

  //constants for null values handling
  val CLICK_STREAM_NOT_NULL_KEYS = Seq("visitor_id","item_id")
  val COLUMN_NAME_DEFAULT_VALUE_CLICK_STREAM_MAP=Map("id"->(-1),"device_type"->"UNKNOWN","session_id"->"UNKNOWN","redirection_source"->"UNKNOWN","is_add_to_cart"->false,"is_order_placed"->false)

  val ITEM_NOT_NULL_KEYS = Seq("item_id", "vendor_id")
  val COLUMN_NAME_DEFAULT_VALUE_ITEM_DATA_MAP=Map("item_price"->(-1),"product_type"->"UNKNOWN","department_name"->"UNKNOWN","vendor_id"->(-1),"vendor_name"->"UNKNOWN")

  //output path
  val CLICK_STREAM_OUTPUT_PATH: String = "spark.app.clickStreamOutputPath"
  val ITEM_OUTPUT_PATH: String = "spark.app.itemDataOutputPath"

  val INPUT_TIME_STAMP_FORMAT = "MM/dd/yyyy HH:mm"
  val DATE_FORMAT = "yyyy-MM-dd"
  val TIME_STAMP_COL = "event_timestamp"
  val REDIRECTION_COL = "redirection_source"
  val EVENT_DATE = "event_d"
  val RECORD_LOAD_TIME = "record_load_ts"

  val FILE_FORMAT = "csv"

  val CLICK_STREAM_DATATYPE = List(
    ("id", "int"),
    ("event_timestamp", "timestamp"),
    ("device_type", "string"),
    ("session_id", "string"),
    ("visitor_id", "string"),
    ("item_id", "string"),
    ("redirection_source", "string"),
    ("is_add_to_cart", "boolean"),
    ("is_order_placed", "boolean"))

  val ITEM_DATATYPE = List(
    ("item_id", "string"),
    ("item_price", "double"),
    ("product_type", "string"),
    ("department_name", "string"),
    ("vendor_id", "int"),
    ("vendor_name", "string"))

  //  Join
  val join_key: Seq[String] = Seq("item_id")
  val join_type: String = "left"

  val ENCRYPTED_DATABASE_PASSWORD: String = "data/encrypted_password.txt"
  val DATABASE_URL: String = "spark.app.databaseURL"
}
