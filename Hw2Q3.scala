val StringA = "Stanford"

val review = sc.textFile("/FileStore/tables/review.csv")

val business = sc.textFile("/FileStore/tables/business.csv")

val business_value=business.distinct.map(line=>line.split(","))

val businessval = business_value.map(line => line(0).split("::")).filter(li=>(li(1).contains("Stanford")))

val businessKeyValue = businessval.map(x => (x(0), x(1)))

val review_value=review.map(line=>line.split("::"))

val reviewKeyValue = review_value.map(x => (x(2), x(1) + "\t" + x(3)))

val joined = businessKeyValue.leftOuterJoin(reviewKeyValue)

dbutils.fs.rm("/question_3",true)
val output = joined.map(x=>(x._2._2)).repartition(1).saveAsTextFile("/question_3")
dbutils.fs.cp("/question_3/part-00000","dbfs:/FileStore/tables/output_q3.txt",true)