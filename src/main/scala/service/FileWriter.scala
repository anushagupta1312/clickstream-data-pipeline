package service

import constants.ApplicationConstants.{DB_SOURCE, DB_USER, ENCRYPTED_DATABASE_PASSWORD, JDBC_DRIVER}
import exceptions.Exceptions.DatabaseException
import org.apache.spark.sql.DataFrame

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.io.Source

object FileWriter {

  def decryptPassword(encryptedPasswordPath: String): String = {
    //read from file and decrypt password
    val source = Source.fromFile(encryptedPasswordPath)
    val encryptedPassword = try source.mkString finally source.close()
    val decoded = Base64.getDecoder.decode(encryptedPassword)

    //return the decrypted password as a string
    val password = new String(decoded, StandardCharsets.UTF_8)
    password
  }

  def fileWriter(databaseURL: String, tableName: String, inputDF: DataFrame): Unit = {
    val password = decryptPassword(ENCRYPTED_DATABASE_PASSWORD)
    try {
      inputDF.write.format(DB_SOURCE)
        .option("url", databaseURL)
        .option("driver", JDBC_DRIVER)
        .option("dbtable", tableName)
        .option("user", DB_USER)
        .option("password", password)
        .mode("overwrite")
        .save()
    }
    catch {
      case _: Exception => throw DatabaseException("Database connection is not established")
    }
  }
}
