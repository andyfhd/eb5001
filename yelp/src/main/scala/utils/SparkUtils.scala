package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Level, Logger}

object SparkUtils {
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ")
  }
  def getSparkContext(appName: String) = {
    var checkpointDirectory = ""

    // get spark configuration
    val conf = new SparkConf()
      .setAppName(appName)

    // Check if running from IDE
    if (isIDE) {
      System.setProperty("hadoop.home.dir", "C:\\Users\\dongdong\\Source\\eb5001\\WinUtils") // required for winutils
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///c:/temp"
    } else {
      checkpointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    // setup spark context
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach( cp => ssc.checkpoint(cp))
    ssc
  }

}
