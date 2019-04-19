package reviewstream

import java.io.FileWriter

import config.Settings
import org.apache.commons.io.FileUtils

import scala.util.Random

object ReviewProducer extends App {
  // WebLog config
  val wlc = Settings.ReviewGen

  val Businesses = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/businesses.csv")).getLines().toArray
  val Users = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/users.csv")).getLines().toArray

  val rnd = new Random()
  val filePath = wlc.filePath
  val destPath = wlc.destPath

  for (fileCount <- 1 to wlc.numberOfFiles) {

    val fw = new FileWriter(filePath, true)

    // introduce some randomness to time increments for demo purposes
    val incrementTimeEvery = rnd.nextInt(wlc.records - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    for (iteration <- 1 to wlc.records) {
      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * wlc.timeMultiplier)
      timestamp = System.currentTimeMillis() // move all this to a function
      val stars = ((iteration + rnd.nextInt(200)) % 5) + 1

      val reviewer = Users(rnd.nextInt(Users.length - 1))
      val business = Businesses(rnd.nextInt(Businesses.length - 1))

      val line = s"$adjustedTimestamp\t$stars\t$reviewer\t$business\n"
      fw.write(line)

      if (iteration % incrementTimeEvery == 0) {
        println(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }

    }
    fw.close()

    val outputFile = FileUtils.getFile(s"${destPath}data_$timestamp")
    println(s"Moving produced data to $outputFile")
    FileUtils.moveFile(FileUtils.getFile(filePath), outputFile)
    val sleeping = 5000
    println(s"Sleeping for $sleeping ms")
  }
}