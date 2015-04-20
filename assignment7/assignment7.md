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

val CONTENT = sc.textFile("/shared/tweets2011.txt")

val CONTENT_SPLIT = CONTENT.flatMap(line => line.split("\n"))

val CONTENT_SPLIT_BY_TAB = CONTENT_SPLIT.map(l => l.split("\t")).filter(l => !(l.length < 4))

val CALENDAR_OBJ = java.util.Calendar.getInstance();

CALENDAR_OBJ.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))

val DATES = CONTENT_SPLIT_BY_TAB.map(l => CALENDAR_OBJ.setTime(new java.util.Date( l(2) )))

val DATES_EDITTED = DATES.map(l => (new String( (CALENDAR_OBJ.get(java.util.Calendar.MONTH)+1) + "/" + CALENDAR_OBJ.get(java.util.Calendar.DAY_OF_MONTH)),
if (CALENDAR_OBJ.get(java.util.Calendar.HOUR_OF_DAY) <= 9) "0"+(CALENDAR_OBJ.get(java.util.Calendar.HOUR_OF_DAY)).toString else CALENDAR_OBJ.get(java.util.Calendar.HOUR_OF_DAY).toString ) )

val DATES_REDUCED = DATES_EDITTED.map(tweets => (tweets, 1)).reduceByKey(_ + _)

val DATES_SORTED = DATES_REDUCED.sortByKey()

DATES_SORTED.collect()

DATES_SORTED.count()

DATES_SORTED.saveAsTextFile("hourly-counts-spark-all")


<h4><u><i>Script for Question 2</i></u></h4>


val CONTENT = sc.textFile("/shared/tweets2011.txt")

val REGEXP_PATTERN = ".*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*".r

val FILTERED_CONTENT = CONTENT.map(line => if (REGEXP_PATTERN.pattern.matcher(line).matches) { line } else { null }).filter(line => line != null)

val CONTENT_SPLIT = FILTERED_CONTENT.flatMap(line => line.split("\n"))

val CONTENT_SPLITBYTAB = CONTENT_SPLIT.map(l => l.split("\t")).filter(l => !(l.length < 4))

val CALENDAR_OBJ = java.util.Calendar.getInstance();

CALENDAR_OBJ.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))

val DATES = CONTENT_SPLITBYTAB.map(l => CALENDAR_OBJ.setTime(new java.util.Date( l(2) )))

val DATES_EDITTED = DATES.map(l => (new String( (CALENDAR_OBJ.get(java.util.Calendar.MONTH)+1) + "/" + CALENDAR_OBJ.get(java.util.Calendar.DAY_OF_MONTH)),
if (CALENDAR_OBJ.get(java.util.Calendar.HOUR_OF_DAY) <= 9) "0"+(CALENDAR_OBJ.get(java.util.Calendar.HOUR_OF_DAY)).toString else CALENDAR_OBJ.get(java.util.Calendar.HOUR_OF_DAY).toString ) )

val DATES_REDUCED = DATES_EDITTED.map(tweets => (tweets, 1)).reduceByKey(_ + _)

val DATES_SORTED = DATES_REDUCED.sortByKey()

DATES_SORTED.collect()

DATES_SORTED.count()

DATES_SORTED.saveAsTextFile("hourly-counts-spark-egypt")

<h3>Output is stored in part files in the respective folders.</h3>
