val userA = "0"
val userB = "12"
val input = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")

val frnd = input.map(li=>li.split("\\t")).filter(l1 => (l1.size == 2)).filter(li=>(userB==li(0))).flatMap(li=>li(1).split(","))
val frnd1 = input.map(li=>li.split("\\t")).filter(l1 => (l1.size == 2)).filter(li=>(userA==li(0))).flatMap(li=>li(1).split(","))

val frnddf = frnd.toDF()
frnddf.createOrReplaceTempView("frnddf")
val frnd1df = frnd1.toDF()
frnd1df.createOrReplaceTempView("frnd1df")

val result = spark.sql("SELECT frnddf.value FROM frnddf INNER JOIN frnd1df ON frnddf.value = frnd1df.value")