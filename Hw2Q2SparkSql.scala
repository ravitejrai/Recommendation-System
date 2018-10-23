import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql._

val list = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")

val details = sc.textFile("/FileStore/tables/UserData.txt")

val details_value=details.map(line=>line.split(","))
val friend_details=details_value.map(e=>(e(0).toInt,e(1),e(2),e(3)))
//val friendskeyVal = friend_details.map(x=> (x(0),x))
val FriendsDf = friend_details.toDF
FriendsDf.createOrReplaceTempView("FriendsDF")

val row=list.filter{_.split("\t").size>1}

val key_value_pairs=row.map(line=>(line.split("\t")(0),line.split("\t")(1).split(",")))

val friend_pair=key_value_pairs.flatMap{
  case(frnd,frndlist)=>{frndlist.map(x=>((x.toInt min frnd.toInt,x.toInt max frnd.toInt),frndlist))}
}
//calculating the mutual friends and then removing all the values where the mutual friends length is 0
val mutualfriends=friend_pair.reduceByKey((a,b)=>a.toSet.intersect(b.toSet).toArray).filter{_._2.length>0}
val mutualfriends_count=mutualfriends.map(line=>(line._1,line._2.length))
val sorted_mutual_friends=mutualfriends_count.map(item => item.swap).sortByKey(false, 1).map(item => item.swap)

var sorted_list=sorted_mutual_friends.take(10)
val sorted_value=sc.parallelize(sorted_list)


//val MutualFriendTable = sorted_value.map(x => (x._1._1,x._1._2))

val MutualFirstKey = sorted_value.map(x => (x._1._1,(x._1._1,x._1._2)))
val MutualFirstKeyDF = MutualFirstKey.toDF
val MutualFirstKeyDFconverted = MutualFirstKeyDF.withColumn("finalKey1", '_2)
MutualFirstKeyDFconverted.createOrReplaceTempView("MutualFirstKeyDFconverted")

val MutualSecondKey = sorted_value.map(x => (x._1._2,(x._1._1,x._1._2)))
val MutualSecondKeyDF = MutualSecondKey.toDF
val MutualSecondKeyDFconverted = MutualSecondKeyDF.withColumn("finalKey2", '_2)
MutualSecondKeyDFconverted.createOrReplaceTempView("MutualSecondKeyDFconverted")

val firstperson = sorted_value.map(x => (x._1._1,x._2))
val firstpersonDF = firstperson.toDF
val firstpersonDFconverted = firstpersonDF.withColumn("count1", '_2.cast("Int"))
firstpersonDFconverted.createOrReplaceTempView("firstpersonDFconverted")

val firstFriendJoin = spark.sql("select FriendsDf._1,FriendsDf._2,FriendsDf._3,FriendsDf._4,firstpersonDFconverted.count1 from FriendsDf, firstpersonDFconverted where FriendsDf._1 = firstpersonDFconverted._1")
firstFriendJoin.createOrReplaceTempView("firstFriendJoin")

val FirstFriendinfo = spark.sql("select firstFriendJoin._2 ,firstFriendJoin._3 ,firstFriendJoin._4 , firstFriendJoin.count1 , MutualFirstKeyDFconverted.finalKey1 from MutualFirstKeyDFconverted, firstFriendJoin where MutualFirstKeyDFconverted._1 = firstFriendJoin._1")
FirstFriendinfo.createOrReplaceTempView("FirstFriendinfo")

val SecondFriend = sorted_value.map(x => (x._1._2,x._2))
val SecondpersonDF = SecondFriend.toDF
val SecondpersonDFconverted = SecondpersonDF.withColumn("count2", '_2.cast("Int"))
SecondpersonDFconverted.createOrReplaceTempView("SecondpersonDFconverted")

val SecondFriendJoin = spark.sql("select FriendsDf._1, FriendsDf._2, FriendsDf._3, FriendsDf._4, SecondpersonDFconverted.count2 from FriendsDf, SecondpersonDFconverted where FriendsDf._1 = SecondpersonDFconverted._1")
SecondFriendJoin.createOrReplaceTempView("SecondFriendJoin")

val SecondFriendinfo = spark.sql("select SecondFriendJoin._2 ,SecondFriendJoin._3 ,SecondFriendJoin._4 , SecondFriendJoin.count2 , MutualSecondKeyDFconverted.finalKey2 from MutualSecondKeyDFconverted, SecondFriendJoin where MutualSecondKeyDFconverted._1 = SecondFriendJoin._1")
SecondFriendinfo.createOrReplaceTempView("SecondFriendinfo")

val FinalOutput = spark.sql("select SecondFriendinfo.count2, FirstFriendinfo._2 ,FirstFriendinfo._3 ,FirstFriendinfo._4, SecondFriendinfo._2 ,SecondFriendinfo._3 ,SecondFriendinfo._4 from SecondFriendinfo, FirstFriendinfo where FirstFriendinfo.finalKey1 = SecondFriendinfo.finalKey2").distinct()