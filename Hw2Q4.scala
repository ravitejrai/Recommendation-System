val review = sc.textFile("/FileStore/tables/review.csv")

val business = sc.textFile("/FileStore/tables/business.csv")

val business_value=business.map(line=>line.split("::"))

val businessKeyValue = business_value.map(x => (x(0), x(2))).distinct()

val newrdd =  business_value.map(x => (x(0),x(1).split(",")(1))).distinct()

val joined = businessKeyValue.join(newrdd)

val businessvaladdressKeyVal = businessvaladdress.map(x =>x(0)) 

val review_value=review.map(l3=>l3.split("::"))

val reviewKeyValue = review_value.map(x => (x(2), x(3).toDouble))

val avgValue = reviewKeyValue.mapValues((_, 1))
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        .mapValues{ 
          case (sum, count) => (1.0 * sum) / count 
        }

val avgsort = avgValue.distinct.sortBy(-_._2).take(10)

val toptenrdd = sc.parallelize(avgsort)

val avgRatingJoined = toptenrdd.join(joined)