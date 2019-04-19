package object domain {
  case class Review(timestamp_hour: Long,
                    stars: String,
                    reviewer: String,
                    business: String//,
                    //inputProps: Map[String, String] = Map()
                     )

  case class RatingByBusiness(business : String,
                              timestamp_hour : Long,
                              one_star_count : Long,
                              two_star_count : Long,
                              three_star_count : Long,
                              four_star_count : Long,
                              five_star_count : Long)

  case class ReviewerByBusiness(business : String, unique_reviewers : Long)
}
