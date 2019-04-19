package streaming

import domain.{ReviewerByBusiness, ReviewByBusiness, Review}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import utils.SparkUtils._
import functions._
import com.twitter.algebird.HyperLogLogMonoid

object StreamingJob {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(4)

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      val inputPath = isIDE match {
        case true => "file:///C:/Users/dongdong/VirtualBox VMs/vagrant_file/vagrant/input"
        case false => "file:///vagrant/input"
      }

      val textDStream = ssc.textFileStream(inputPath)

      val reviewStream = textDStream.transform(input => {
        input.flatMap { line =>
          val record = line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 4)
            Some(Review(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3)))
          else
            None
        }
      }).cache()

      // review by business
      val reviewStateSpec =
        StateSpec
          .function(mapReviewStateFunc)
          .timeout(Minutes(120))

      val statefulReviewByBusiness = reviewStream.transform(rdd => {
        val df = rdd.toDF()
        df.registerTempTable("review")
        val reviewByBusiness = sqlContext.sql(
          """SELECT
                                            business,
                                            timestamp_hour,
                                            sum(case when stars = '1' then 1 else 0 end) as one_star_count,
                                            sum(case when stars = '2' then 1 else 0 end) as two_star_count,
                                            sum(case when stars = '3' then 1 else 0 end) as three_star_count,
                                            sum(case when stars = '4' then 1 else 0 end) as four_star_count,
                                            sum(case when stars = '5' then 1 else 0 end) as five_star_count
                                            from review
                                            group by business, timestamp_hour """)
        reviewByBusiness
          .map { r => ((r.getString(0), r.getLong(1)),
            ReviewByBusiness(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5), r.getLong(6))
            )
          }
      }).mapWithState(reviewStateSpec)

      val reviewStateSnapshot = statefulReviewByBusiness.stateSnapshots()
      reviewStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4),
          filterFunc = (record) => false
        )
        .foreachRDD(rdd => rdd.map(sr => ReviewByBusiness(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3, sr._2._4, sr._2._5))
          .toDF().registerTempTable("ReviewByBusiness"))


      // unique reviewers by business
      val reviewerStateSpec =
        StateSpec
        .function(mapReviewersStateFunc)
        .timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)
      val statefulReviewersByBusiness = reviewStream.map( a => {
        ((a.business, a.timestamp_hour), hll(a.reviewer.getBytes))
      } ).mapWithState(reviewerStateSpec)

      val reviewerStateSnapshot = statefulReviewersByBusiness.stateSnapshots()
      reviewerStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4),
          filterFunc = (record) => false
        ) // only save or expose the snapshot every x seconds
        .foreachRDD(rdd => rdd.map(sr => ReviewerByBusiness(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
        .toDF().registerTempTable("ReviewerByBusiness"))

      statefulReviewByBusiness.print(10)
      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()

  }

}
