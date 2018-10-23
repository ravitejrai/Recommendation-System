val list = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")

val details = sc.textFile("/FileStore/tables/UserData.txt")

val details_value=details.map(line=>line.split(",")).map(e=>(e(0).toInt,e(1),e(2),e(3)))

val friend_details=details_value.map(e=>(e(0).toInt,e(1),e(2),e(3)))

val row=list.filter{_.split("\t").size>1}

val key_value_pairs=row.map(line=>(line.split("\t")(0),line.split("\t")(1).split(",")))

val friend_pair=key_value_pairs.flatMap{
  case(frnd,frndlist)=>{frndlist.map(x=>((x.toInt min frnd.toInt,x.toInt max frnd.toInt),frndlist))}
}

val mutualfriends=friend_pair.reduceByKey((a,b)=>a.toSet.intersect(b.toSet).toArray).filter{_._2.length>0}
val mutualfriends_count=mutualfriends.map(line=>(line._1,line._2.length))
val sorted_mutual_friends=mutualfriends_count.map(item => item.swap).sortByKey(false, 1).map(item => item.swap)

var sorted_list=sorted_mutual_friends.take(10)
val sorted_value=sc.parallelize(sorted_list)

val first_friend = sorted_value.keyBy( x => x._1._1) 
val first_friend_details = friend_details.keyBy(x => x._1)
val first_friend_joined=first_friend.join(first_friend_details)

val second_friend = sorted_value.keyBy( x => x._1._2) 
val second_friend_details = friend_details.keyBy(x => x._1)
val second_friend_joined=second_friend.join(second_friend_details)

val first_friend_joined_key=first_friend_joined.keyBy(x=>x._2._1)
val second_friend_joined_key=second_friend_joined.keyBy(x=>x._2._1)
val mutual_friends_joined=first_friend_joined_key.join(second_friend_joined_key)
val final_mutual_friends=mutual_friends_joined.map(e=>(e._1._2,e._2._1._2._2,e._2._2._2._2))

val count= final_mutual_friends.map(e=>e._1)
val mutual_first_friend= final_mutual_friends.map(e=>e._2)
val mutual_second_friend= final_mutual_friends.map(e=>e._3)

dbutils.fs.rm("/question_2",true)
final_mutual_friends.map(x =>( x._1+"\t"+x._2._2+"\t"+x._2._3+"\t"+x._2._4+"\t"+x._3._2+"\t"+x._3._3+"\t"+x._3._4)).repartition(1).saveAsTextFile("/question_2")
dbutils.fs.cp("/question_2/part-00000","dbfs:/FileStore/tables/output_q2.txt",true)