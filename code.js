// make a connection to the database server
conn = new Mongo();

// set the default database
db = conn.getDB("a1");



//Q1
// Find out the number of general tweets, replies and retweets in the data set. A
// general tweet is a tweet with no replyto_id, nor retweet_id field; a reply is a tweet
// with the replyto_id field; a retweet is a tweet with the retweet_id field.

var General_Tweet = db.tweets.aggregate([{$match:{replyto_id:{$exists: false},retweet_id:{$exists: false}}},{$group:{_id:null,count:{$sum:1}}}]).toArray()[0]["count"]
var Reply = db.tweets.aggregate([{$match:{replyto_id:{$exists: true},retweet_id:{$exists: false}}},{$group:{_id:null,count:{$sum:1}}}]).toArray()[0]["count"]
var Retweet = db.tweets.aggregate([{$match:{replyto_id:{$exists: false},retweet_id:{$exists: true}}},{$group:{_id:null,count:{$sum:1}}}]).toArray()[0]["count"]
// 
db.result.insert({"General Tweet":General_Tweet,"Reply":Reply,"Retweet":Retweet})
var cursor = db.result.find()
while(cursor.hasNext()){printjson(cursor.next())};
db.result.drop()


//Q2
//  Find out the top 5 hashtags sorted by their occurrence in general or reply tweets.
// We do not count retweet, which has the same textual content as the parent tweet.

var cursor = db.tweets.aggregate([{$match:{hash_tags:{$exists: true},retweet_id:{$exists: false}}},{$unwind: "$hash_tags"},{$group:{_id:{text:"$hash_tags.text"},count:{$sum:1}}},{$sort:{count:-1}},{$limit:5}]);

while(cursor.hasNext()){printjson(cursor.next())};

//Q3
// Find out the tweet taking the longest time to receive a reply; print out the id
// and the duration between the tweet’s creation time and the creation time of its first
// reply in second

db.tweets.aggregate([
{$match:{"replyto_id":{$exists:true}}},
{$group:{_id:"$replyto_id",earlist_time:{$min:"$created_at"}}},
{$lookup:{from:"tweets",localField:"_id",foreignField:"id",as:"new_doc"}}
,{$project:{id:"$_id",time1:"$earlist_time",time2:"$new_doc.created_at"}}
,{$out:"result"}
])

db.result.aggregate([
{$unwind:"$time2"}
,{$out:"result1"}
])

var cursor = db.result1.aggregate([
{$project:{id:"$id",time:{$subtract:[{$dateFromString:{dateString:"$time1"}},{$dateFromString:{dateString:"$time2"}}]}}}
,{$sort:{"time":-1}}
,{$limit:1}
])

while(cursor.hasNext()){printjson(cursor.next())};
db.result.drop()
db.result1.drop()



//Q4
//  Find out the number of general and reply tweets that do not have all their
// retweets included in the data set. Note that the total number of retweets a tweet is
// stored in the field retweet_count

db.tweets.aggregate([
{$match:{"retweet_id":{$exists:true}}},
{$group:{_id:"$retweet_id",count:{$sum:1}}}
,{$sort:{count:-1}}
,{$out:"result"}
])


db.tweets.aggregate([
{$match:{$or:[{$and:[{replyto_id:{$exists: false}},{retweet_id:{$exists: false}}]},{$and:[{replyto_id:{$exists: true},retweet_id:{$exists: false}}]}]}}
,{$lookup:{from:"result",localField:"id",foreignField:"_id",as:"new_doc"}}
,{$project:{_id:"$id",count1:"$retweet_count",count2:"$new_doc.count"}}
,{$out:"result1"}
])

db.result1.updateMany({"count2":[]},{$set:{"count2":0}})

db.result1.aggregate([
{$unwind:"$count2"}
,{$sort:{count2:-1}}
,{$out:"result2"}
])

var cursor = db.result2.aggregate([
{$match:{"$expr": { "$gt": [ "$count1" , "$count2" ]}}}
,{$group:{_id:null,count:{$sum:1}}}
])

while(cursor.hasNext()){printjson(cursor.next())};

db.result.drop()
db.result1.drop()
db.result2.drop()




//Q5
// Find out the number of tweets that do not have its parent tweet object in the
// data set

db.tweets.aggregate([
{$group:{_id:"$id"}}
,{$out:"result"}
])


db.tweets.aggregate([
{$match:{retweet_id:{$exists: true}}},
{$lookup:{from:"result",localField:"retweet_id",foreignField:"_id",as:"new_doc"}}
,{$project:{_id:"$id",count:"$new_doc._id"}}
,{$out:"result1"}
])

db.tweets.aggregate([
{$match:{replyto_id:{$exists: true}}},
{$lookup:{from:"result",localField:"replyto_id",foreignField:"_id",as:"new_doc"}}
,{$project:{_id:"$id",count:"$new_doc._id"}}
,{$out:"result2"}
])

db.result1.updateMany({"count":[]},{$set:{"count":0}})
db.result2.updateMany({"count":[]},{$set:{"count":0}})

db.result1.aggregate([
{$unwind:"$count"}
,{$out:"result3"}
])

db.result2.aggregate([
{$unwind:"$count"}
,{$out:"result4"}
])

var count_a = db.result3.aggregate([
{$match:{count:0}},
{$group:{_id:null,count:{$sum:1}}}
]).toArray()[0]["count"]

var count_b = db.result4.aggregate([
{$match:{count:0}},
{$group:{_id:null,count:{$sum:1}}}
]).toArray()[0]["count"]

db.result.drop()
db.result1.drop()
db.result2.drop()
printjson(count_a+count_b)

//Q6
//  Find out the number of general tweets that do not have a reply nor a retweet in
// the data set

db.tweets.aggregate([
{$match:{replyto_id:{$exists:true}}},
{$group:{_id:"$replyto_id"}}
,{$out:"result"}
])
db.tweets.aggregate([
{$match:{retweet_id:{$exists:true}}},
{$group:{_id:"$retweet_id"}}
,{$out:"result1"}
])


db.tweets.aggregate([
{$match:{replyto_id:{$exists: false},retweet_id:{$exists: false}}},
{$lookup:{from:"result",localField:"id",foreignField:"_id",as:"reply_count"}}
,{$lookup:{from:"result1",localField:"id",foreignField:"_id",as:"retweet_count"}}
,{$project:{_id:"$id",reply_count:"$reply_count._id",retweet_count:"$retweet_count._id"}}
,{$out:"result2"}
])



db.result2.updateMany({$and:[{"reply_count":[]},{"retweet_count":[]}]},{$set:{"count":0}})

var value = db.result2.aggregate([
{$match:{count:0}},
{$group:{_id:null,count:{$sum:1}}}
]).toArray()[0]["count"]
printjson(value)
db.result.drop()
db.result1.drop()
db.result2.drop()
db.result3.drop()
db.result4.drop()
