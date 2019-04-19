package streaming

import com.twitter.algebird.HyperLogLogMonoid
import domain.{Review, ReviewByBusiness, ReviewerByBusiness}
import functions._
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import utils.SparkUtils._

object StreamingJob2 {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(8)

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)
      val flumeStream = FlumeUtils.createStream(ssc, "localhost", 8122)
        .map(e => new String(e.event.getBody.array))
        .cache()

      flumeStream.foreachRDD(rdd => {
        println(s"batch received at: ${System.currentTimeMillis()}, total reviews: ${rdd.count()}")
      })

      val reviewStream = flumeStream.transform(input => {
        input.flatMap { line =>
          val record = line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 4)
            Some(Review(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3)))
          else
            None
        }
      }).cache()




      //flumeStream.map(e => "Event:header:" + e.event.get(0).toString
       // + "body: " + new String(e.event.getBody.array)).print

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()

  }

}
