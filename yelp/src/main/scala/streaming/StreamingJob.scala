package streaming

import com.twitter.algebird.HyperLogLogMonoid
import domain.{Review, RatingByBusiness, ReviewerByBusiness}
import functions._
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import utils.SparkUtils._

object StreamingJob {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuringSecond = 8
    val windowDuringSecond = 30

    val batchDuration = Seconds(batchDuringSecond)

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)
      val flumeStream = FlumeUtils.createStream(ssc, "localhost", 8122)
        .map(e => new String(e.event.getBody.array))
        .cache()

      flumeStream.foreachRDD(rdd => {
        println(s"batch received at: ${System.currentTimeMillis()}, total reviews: ${rdd.count()}")
        rdd.foreach(line => println(line))
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

      // rating by business
      val ratingStateSpec =
        StateSpec
          .function(mapRatingStateFunc)
          .timeout(Minutes(120))

      val statefulRatingByBusiness = reviewStream.transform(rdd => {
        val df = rdd.toDF()
        df.registerTempTable("review")
        val ratingByBusiness = sqlContext.sql(
          """SELECT
                                            business,
                                            timestamp_hour,
                                            sum(case when stars = '1.0' then 1 else 0 end) as one_star_count,
                                            sum(case when stars = '2.0' then 1 else 0 end) as two_star_count,
                                            sum(case when stars = '3.0' then 1 else 0 end) as three_star_count,
                                            sum(case when stars = '4.0' then 1 else 0 end) as four_star_count,
                                            sum(case when stars = '5.0' then 1 else 0 end) as five_star_count
                                            from review
                                            group by business, timestamp_hour """)
        ratingByBusiness
          .map { r => ((r.getString(0), r.getLong(1)),
              RatingByBusiness(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5), r.getLong(6))
            )
          }
      }).mapWithState(ratingStateSpec)


      val ratingStateSnapshot = statefulRatingByBusiness.stateSnapshots()
      ratingStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(windowDuringSecond / batchDuringSecond * batchDuringSecond),
          filterFunc = (record) => false
        )
        .foreachRDD(rdd => rdd.map(sr => RatingByBusiness(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3, sr._2._4, sr._2._5))
          .toDF().registerTempTable("RatingByBusiness"))

      ratingStateSnapshot.foreachRDD(rdd => {
        println(s"ratingStateSnapshot: ${rdd.count()}")
        rdd.foreach(sr => println(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3, sr._2._4, sr._2._5))
      })

      // unique reviewers by business
      val reviewerStateSpec =
        StateSpec
          .function(mapReviewersStateFunc)
          .timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)
      val statefulReviewersByBusiness = reviewStream.map( a => {
        (a.business, hll(a.reviewer.getBytes))
      } ).mapWithState(reviewerStateSpec)

      val reviewerStateSnapshot = statefulReviewersByBusiness.stateSnapshots()
      reviewerStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(windowDuringSecond / batchDuringSecond * batchDuringSecond),
          filterFunc = (record) => false
        ) // only save or expose the snapshot every x seconds
        .foreachRDD(rdd => rdd.map(sr => ReviewerByBusiness(sr._1, sr._2.approximateSize.estimate))
        .toDF().registerTempTable("ReviewerByBusiness"))

//      val topTenBusiness = sqlContext.sql(
//        """SELECT
//                                            *
//                                            from ReviewerByBusiness
//                                            """)
//      topTenBusiness.printSchema()
//      topTenBusiness.foreach(line => println(line))

//      reviewerStateSnapshot.foreachRDD(rdd => {
//        println(s"reviewerStateSnapshot: ${rdd.count()}")
//        rdd.foreach(sr => println(sr._1, sr._2.approximateSize.estimate))
//      })

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()

  }

}
