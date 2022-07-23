package constants

object constants {
  val path_clickstream = "data/clickstream_log.csv"
  val path_item = "data/item_data.csv"

  val datatype_clickstream = Seq(("id", "long"),
    ("event_timestamp", "timestamp"),
    ("device_type", "string"),
    ("session_id", "string"),
    ("visitor_id", "string"),
    ("item_id", "string"),
    ("redirection_source", "string"),
    ("is_add_to_cart", "boolean"),
    ("is_order_placed", "boolean"))

  val datatype_item = Seq(("item_id", "string"),
    ("item_price", "float"),
    ("product_type", "string"),
    ("department_name", "string"),
    ("vendor_id", "int"),
    ("vendor_name", "string"))
}
