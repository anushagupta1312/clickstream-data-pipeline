import org.scalatest.flatspec.AnyFlatSpec

class FileReaderTest extends AnyFlatSpec{
  implicit val spark = utils.ApplicationUtils.createSparkSession();
  import spark.implicits._

  "file reader " should "return a return a dataframe with records " in {
    val inputPath = "data/clickstream_log.csv"
    val inputType = "csv"
    val outputDF = service.FileReader.fileReader(inputPath, inputType)
    outputDF.show()

  }


}
