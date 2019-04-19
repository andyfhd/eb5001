package config

import com.typesafe.config.ConfigFactory

object Settings {
  private val config = ConfigFactory.load()

  object ReviewGen {
    private val weblogGen = config.getConfig("reviewstream")

    lazy val records = weblogGen.getInt("records")
    lazy val timeMultiplier = weblogGen.getInt("time_multiplier")
    lazy val filePath = weblogGen.getString("file_path")
    lazy val destPath = weblogGen.getString("dest_path")
    lazy val numberOfFiles = weblogGen.getInt("number_of_files")

  }
}
