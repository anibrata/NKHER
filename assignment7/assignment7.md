-----------------------
Assignment 7 Answers
-----------------------


Pig Scripts 
-------------

<h4><u><i>Script for Question 1</i></u></h4>

A = load '/shared/tweets2011.txt' AS (tweetId:long, userId:chararray, date:chararray, text:chararray);

A = FILTER A by ($0 IS NOT NULL) AND ($1 IS NOT NULL) AND ($2 IS NOT NULL) AND ($3 IS NOT NULL);

B = FOREACH A GENERATE ToDate(date,'EEE MMM d HH:mm:ss Z yyyy', 'GMT') AS date,userId,text,tweetId;

C = FOREACH B {

MONTH = (chararray)GetMonth(date);

DAY = (chararray)GetDay(date); 

HOUR = (int) GetHour(date);

GENERATE CONCAT(CONCAT(MONTH,'/'),CONCAT(DAY,' ')) AS md, ( HOUR<=9 ? CONCAT('0',(chararray)HOUR):(chararray)HOUR ) AS h;

};

D = GROUP C BY (md, h);

D = ORDER D BY group ASC;

E = FOREACH D GENERATE group AS dateTimeHour, COUNT(C) AS count;

STORE E INTO 'hourly-counts-pig-all';

DUMP E;


<h4><u><i>Script for Question 2</i></u></h4>


A = load '/shared/tweets2011.txt' AS (tweetId:long, userId:chararray, date:chararray, text:chararray);

A = FILTER A by ($0 IS NOT NULL) AND ($1 IS NOT NULL) AND ($2 IS NOT NULL) AND ($3 IS NOT NULL);

B = FOREACH A GENERATE ToDate(date,'EEE MMM d HH:mm:ss Z yyyy', 'GMT') AS date,userId,text,tweetId;

B = FILTER B BY (text matches '.\*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).\*' );

C = FOREACH B {

MONTH = (chararray)GetMonth(date);

DAY = (chararray)GetDay(date); 

HOUR = (int) GetHour(date);

GENERATE CONCAT(CONCAT(MONTH,'/'),CONCAT(DAY,' ')) AS md, ( HOUR<=9 ? CONCAT('0',(chararray)HOUR):(chararray)HOUR ) AS h;

};

D = GROUP C BY (md, h);

D = ORDER D BY group ASC;

E = FOREACH D GENERATE group AS dateTimeHour, COUNT(C) AS count;

STORE E INTO 'hourly-counts-pig-egypt';

DUMP E;


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
