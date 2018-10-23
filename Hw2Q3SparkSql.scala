val review = spark.sql("SELECT review_csv._c0,review_csv._c2,review_csv._c4,review_csv._c6 FROM review_csv")
val business = spark.sql("SELECT business_csv._c0,business_csv._c2,business_csv._c4 FROM business_csv").distinct

val reduceddf = business.filter($"_c2".contains("Stanford"))

val joineddf  = review.select('_c0 as "review_c0" , '_c2 as "review_c2", '_c4 as "review_c4" , '_c6 as "review_c6").join(reduceddf.select('_c0 as "business_c0", '_c2 as "business_c2", '_c4 as "business_c4"), $"review_c4" === $"business_c0")

joineddf.createOrReplaceTempView("joineddf")

val selectedjoin = spark.sql("SELECT joineddf.review_c2,joineddf.review_c6 FROM joineddf")

dbutils.fs.rm("/question_3_SparkSql",true)
val output = selectedjoin.rdd.map(_.toString()).saveAsTextFile("/question_3_SparkSql")
dbutils.fs.cp("/question_3_SparkSql/part-00000","dbfs:/FileStore/tables/output_q3_SparkSql.txt",true)