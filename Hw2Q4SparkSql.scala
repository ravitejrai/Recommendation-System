import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions._

val review = spark.sql("SELECT review_csv._c4,review_csv._c6 FROM review_csv")

val business = spark.sql("SELECT business_csv._c0,business_csv._c2,business_csv._c4 FROM business_csv").distinct

business.createOrReplaceTempView("business")

val reviewconverted = review.withColumn("Ratings", '_c6.cast("Double"))

reviewconverted.createOrReplaceTempView("reviewconverted")

val newreview = spark.sql("SELECT reviewconverted._c4, reviewconverted.Ratings FROM reviewconverted")

val grouped = newreview.groupBy("_c4").agg(sum("Ratings"), count("_c4"))

val newgroupedDF = grouped.withColumn("average", $"sum(Ratings)" / $"count(_c4)") 

newgroupedDF.createOrReplaceTempView("newgroupedDF")

val finaltable = spark.sql("SELECT newgroupedDF._c4 , newgroupedDF.average FROM newgroupedDF")

val sortedTable = finaltable.sort(desc("average"))

sortedTable.createOrReplaceTempView("sortedTable")

val joineddf  = spark.sql("select business._c0,business._c2,business._c4,sortedtable.average from sortedTable, business where sortedTable._c4 = business._c0 LIMIT 10")

joineddf.createOrReplaceTempView("joineddf")

dbutils.fs.rm("/question_4_SparkSql",true)
val output = joineddf.rdd.map(_.toString()).saveAsTextFile("/question_4_SparkSql")
dbutils.fs.cp("/question_4_SparkSql/part-00000","dbfs:/FileStore/tables/output_q4_SparkSql.txt",true)