-----------------------
Assignment 7 Answers
-----------------------


Pig Scripts 
-------------

<h4><u><i>Script for Question 1</i></u></h4>

a = load '/shared/tweets2011.txt' AS (tweetId:long, userId:chararray, date:chararray, text:chararray);

a = FILTER a by ($0 is not null) AND ($1 is not null) AND ($2 is not null) AND ($3 is not null);

b = FOREACH a GENERATE ToDate(date,'EEE MMM d HH:mm:ss Z yyyy', 'GMT') AS date,userId,text,tweetId;

c = foreach b {  

month = (chararray)GetMonth(date);                         
day = (chararray)GetDay(date);
hour = (chararray) GetHour(date);
generate CONCAT(CONCAT(CONCAT(month,'/'),CONCAT(day,' ')),hour) as modifiedDateTime;                                

};

d = group c by modifiedDateTime;

d = order d by group ASC;

e = foreach d generate group as dateTimeHour, COUNT(c) as count;

store e into 'hourly-counts-pig-all';

dump e;

<h4><u><i>Script for Question 2</i></u></h4>

a = load '/shared/tweets2011.txt' AS (tweetId:long, userId:chararray, date:chararray, text:chararray);

a = FILTER a by ($0 is not null) AND ($1 is not null) AND ($2 is not null) AND ($3 is not null);

b = FOREACH a GENERATE ToDate(date,'EEE MMM d HH:mm:ss Z yyyy', 'GMT') AS date,userId,text,tweetId;

b = FILTER b BY (text matches '.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*' );

c = foreach b {  

month = (chararray)GetMonth(date);                         
day = (chararray)GetDay(date);
hour = (chararray) GetHour(date);
generate CONCAT(CONCAT(CONCAT(month,'/'),CONCAT(day,' ')),hour) as modifiedDateTime;                                

};

d = group c by modifiedDateTime;

d = order d by group ASC;

e = foreach d generate group as dateTimeHour, COUNT(c) as count;

store e into 'hourly-counts-pig-egypt';

dump e;

Spark Scripts 
--------------


<h4><u><i>Script for Question 1</i></u></h4>

val tweets = sc.textFile("/shared/tweets2011.txt")

val tweets_splitByLines = tweets.flatMap(line => line.split("\n"))

val tweets_splitByTab = tweets_splitByLines.map(l => l.split("\t")).filter(l => !(l.length < 4))

val cal = java.util.Calendar.getInstance();

cal.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))

val tweetDates = tweets_splitByTab.map(l => cal.setTime(new java.util.Date( l(2) ))).filter(l => l != null)

val formattedDates = tweetDates.map(l => (new String( (new java.text.SimpleDateFormat("M").format(cal.getTime())) + "/" + cal.get(java.util.Calendar.DAY_OF_MONTH)), (cal.get(java.util.Calendar.HOUR_OF_DAY)) ))

val reduceDates = formattedDates.map(tweets => (tweets, 1)).reduceByKey(_ + _)

val sortedReducedDates = reduceDates.sortByKey()

sortedReducedDates.collect()

sortedReducedDates.saveAsTextFile("hourly-counts-spark-all")


<h4><u><i>Script for Question 2</i></u></h4>


val tweets = sc.textFile("/shared/tweets2011.txt")

val regexp = ".*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*".r

tweets filter (line => regexp.pattern.matcher(line).matches)

val filteredTweets = tweets.map(line => regexp.findAllIn(line))

val filteredTweets = tweets.map(line =>
if (regexp.pattern.matcher(line).matches) {
    line
}
else {
    null
}).filter(line => line != null)



val tweets_splitByLines = tweets.flatMap(line => line.split("\n"))

val filteredTweets_splitByTab = filteredTweets.map(l => l.split("\t")).filter(l => !(l.length < 4))

val cal = java.util.Calendar.getInstance();

cal.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))

val tweetDates = filteredTweets_splitByTab.map(l => cal.setTime(new java.util.Date( l(2) )))

val formattedDates = tweetDates.map(l => (new String( (new java.text.SimpleDateFormat("M").format(cal.getTime())) + "/" + cal.get(java.util.Calendar.DAY_OF_MONTH)), (cal.get(java.util.Calendar.HOUR_OF_DAY)) ))

val reduceDates = formattedDates.map(tweets => (tweets, 1)).reduceByKey(_ + _)

val sortedReducedDates = reduceDates.sortByKey()

sortedReducedDates.collect()

sortedReducedDates.saveAsTextFile("hourly-counts-spark-egypt")




<h3>Output is stored in part files in the respective folders.</h3>
