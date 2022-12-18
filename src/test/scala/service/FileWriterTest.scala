package service

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import service.FileWriter._

import java.io.{File, PrintWriter}

class FileWriterTest extends AnyFlatSpec {

  implicit val spark: SparkSession = helper.Helper.createSparkSession()

  "decrypt password " should "decrypt password from the encrypted password file" in {
    //create a file with encrypted password "cGFzczEyMw=="
    val ePass = "src/test/scala/e_password.txt"
    val ePassFile = new File(ePass)
    val writer = new PrintWriter(ePassFile)
    writer.write("cGFzczEyMw==")
    writer.close()

    //check if decrypted password matches original
    val decryptedPassword = decryptPassword(ePass)
    assert("pass123" == decryptedPassword)

    //delete temp file created for test
    ePassFile.delete()
  }

  "file writer " should "write a dataframe to a table " in {
    val inputDF = service.FileReader.fileReader(helper.Helper.CLICK_STREAM_TEST_INPUT_PATH, "csv")
    val tableName = "unittest"
    val dbURL = helper.Helper.DATABASE_TEST_URL
    fileWriter(dbURL, tableName, inputDF)
  }

}