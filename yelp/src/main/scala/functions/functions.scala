import com.twitter.algebird.{HLL, HyperLogLogMonoid}
import domain.RatingByBusiness
import org.apache.spark.streaming.State

package object functions {
  def mapRatingStateFunc = (k: (String, Long), v: Option[RatingByBusiness], state: State[(Long, Long, Long, Long, Long)]) => {
    var (one_star_count, two_star_count, three_star_count, four_star_count, five_star_count) = state.getOption().getOrElse((0L, 0L, 0L, 0L, 0L))

    val newVal = v match {
      case Some(a: RatingByBusiness) => (a.one_star_count, a.two_star_count, a.three_star_count, a.four_star_count, a.five_star_count)
      case _ => (0L, 0L, 0L, 0L, 0L)
    }

    one_star_count += newVal._1
    two_star_count += newVal._2
    three_star_count += newVal._3
    four_star_count += newVal._4
    five_star_count += newVal._5

    state.update((one_star_count, two_star_count, three_star_count, four_star_count, five_star_count))

    val underExposed = {
      if (one_star_count ==0)
        0
      else
        three_star_count / one_star_count
    }
    underExposed
  }

  def mapReviewersStateFunc = (k: String, v: Option[HLL], state: State[HLL]) => {
    val currentReviewerHLL = state.getOption().getOrElse(new HyperLogLogMonoid(12).zero)
    val newReviewerHLL = v match {
      case Some(reviewerHLL) => currentReviewerHLL + reviewerHLL
      case None => currentReviewerHLL
    }
    state.update(newReviewerHLL)
    val output = newReviewerHLL.approximateSize.estimate
    output
  }

}
