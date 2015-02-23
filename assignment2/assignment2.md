-------------------------------
Assignment 2 Answers
-------------------------------

------------------------
NORMAL IMPLEMENTATION
------------------------

---------------------------
For Bible Shakes
---------------------------

Answer 0. 
Pairs Approach : The implementation has two jobs. First jobs Mapper params <LongWritable, Text, Text, IntWritable>. This does a Word Count per sentence. Reducer params <Text, IntWritable, Text, IntWritable>. The output is stored in a file which is read by the next job. The second job's Mapper params are <LongWritable, Text, PairOfStrings, DoubleWritable>. Mapper calculates the unique cooccurences in each line and outputs the pair and one as count per line. (There are no duplicate pairs as I am getting unique words in a line and sorting them by using a TreeSet in Java). The combiner params are <PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable> and does a key wise sum where the key is a pair of strings and sends to reducer. The Reducer params are <PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable>. The reducer in its setup() method reads the output of the first job and stores the word counts in a hash map (Loading Side data). The reduce function int he reducer calculates the total sum of pairs and then the pmi for them with the help of the hashmap created in the setup and emit's it!

Stripes Approach : The implementation again has two jobs where. First job's Mapper params <LongWritable, Text, Text, IntWritable>. This does a Word Count per sentence. Reducer params <Text, IntWritable, Text, IntWritable>. The output is stored in a file which is read by the next job. The second job's Mapper params are <LongWritable, Text, PairOfStrings, DoubleWritable>. Mapper takes params <LongWritable, Text, Text, HMapStIW> and calculates a hashmap for each word in a sentence. The hashmap for each word/text contains all other unique words it coccurs with and value as 1. 
For example if sentence is "Messi is a a football player" then it removes duplicates into a set <Messi is a football player> and then creates hashmap for each word eg for Messi the hashmap would be {is->1, a->, footballer->1, player->1}. The hashmap for the word "is" will not contain Messi as a key. Key values pairs in the form of (Text, HMapStIW) is passed to the combiners which does a sum and passed to the reducers (Using a TreeSet to sort the unique words of a sentence). The reducer setup method reads the output of the first mapper (dictionary) and stores it in a hashmap. This hashmap is used for individual word count in the reduce() method of the reducer to calculate PMI! (Here the inner working uses the Stripes concept, however the output is kept in terms of pairs for easier analyzing of the outpt file)

Answer 1. With Combiner. Local Machine.
Running time PairsPMI (Job 1 -> 4.807 and Job 2 -> 34.544 seconds) 

JOB 1 --> Input Records -> 156215, Key Value Pairs -> 1531520, Output -> 41788 reduce o/p rec
JOB 2 --> Input Records -> 156215, Key Value Pairs -> 9985193, Output -> 116759 reduce o/p rec

Running Time StripesPMI ((Job 1 -> 4.82 and Job 2 -> 12.468 seconds) 

JOB 1 --> Input Records -> 156215, Key Value Pairs -> 1531520, Output -> 41788 reduce o/p rec
JOB 2 --> Input Records -> 156215, Key Value Pairs -> 1531520, Output -> 116759 reduce o/p rec

Answer 2. Running without combiners. Local Machine.
Running time PairsPMI (Job 1 -> 5.87 and Job 2 -> 39.531 seconds)

JOB 1 --> Input Records -> 156215, Key Value Pairs -> 1531520, Output -> 41788 reduce o/p rec
JOB 2 --> Input Records -> 156215, Key Value Pairs -> 9985193, Output -> 116759 reduce o/p rec

Running Time StripesPMI (Job 1 -> 5.82 and Job 2 -> 14.46 seconds)

JOB 1 --> Input Records -> 156215, Key Value Pairs -> 1531520, Output -> 41788 reduce o/p rec
JOB 2 --> Input Records -> 156215, Key Value Pairs -> 1531520, Output -> 116759 reduce o/p rec

Answer 3.  116759 distinct pairs.

Answer 4. Pair with highest PMI value is (meshach, shadrach) and its PMI value is 4.047594697476721. Having a large PMI between these two terms signifies that when you see either word you know more about the probability of seeing the other word. Or seeing "meshach" gives you highest information about seeing "shadrach". There are other Pairs also that have similar values which are :
(meshach, abednego) and (abednego, shadrach)

Answer 5. 

TOP THREE WORDS WITH CLOUD

(cloud, tabernacle) 1.803635857731193
(cloud, glory) 1.476112742132996
(cloud, fire) 1.4051478433310451

TOP THREE WORDS WITH LOVE

(hate, love) 1.1185408785362676
(hermia, love) 0.8811799627416638
(commandments, love) 0.8423344665973186


-------------------------------------------
For simplewiki-20141222-pages-articles.txt
-------------------------------------------

Answer 0. Same as bible+shakes

Answer 1. With Combiner
Running time Pairs (Job 1 -> 62.928 Job 2 -> 11552.575 seconds) Run on Cluster. With 1 Reducer.

JOB 1 --> Input Records -> 114863, Key Value Pairs -> 11255671, Output -> 1064960 reduce o/p rec
JOB 2 --> Input Records -> 114863, Key Value Pairs -> 1910190399, Output -> 19105212 reduce o/p rec

Running Time Stripes (Job 1 -> 60.995 Job 2 -> 2845.837 seconds) Run on Cluster

JOB 1 --> Input Records -> 114863, Key Value Pairs -> 11255671, Output -> 1064960 reduce o/p rec
JOB 2 --> Input Records -> 114863, Key Value Pairs -> 11255671, Output -> 19105212 reduce o/p rec

Answer 2. Running without combiners. 

Running time Pairs (Job 1 ->  80.123 sec Job 2 -> 3124.23 seconds), Job 1 output -> 41788 reduce output records
Running Time Stripes (Job 1 ->  63.245 Job 2 -> 3433.23 seconds), Job 2 output -> 116759 reduce output records 

All other values are same as for the with combiner version.

Answer 3. 19105212 distinct pairs.

Answer 4. A lot of pairs shared the highest PMI value which are below.

(gridcolor:lightgrey, id:darkgrey)    4.193722733154959
(gridcolor:lightgrey, id:lightgrey)    4.193722733154959
(gridcolor:lightgrey, x.y)    4.193722733154959
(id:barra, id:darkgrey)    4.193722733154959
(id:barra, id:lightgrey)    4.193722733154959
(id:barra, x.y)    4.193722733154959
(id:darkgrey, id:lightgrey)    4.193722733154959
(id:darkgrey, x.y)    4.193722733154959
(id:lightgrey, x.y)    4.193722733154959
((Pantone, TPX)    4.193722733154959

Answer 5. 

TOP WORDS WITH CLOUD

(cloud, nebula.)    2.657623618475959
(cloud, cloud.)    2.61259664866264
(cloud, thunderstorm)    2.4048476173795423
(cloud, convection)    2.3064270133462474
(cloud, clouds)    2.2455121884855473
(cloud, nebula)    2.231654886203678
(cloud, helium)    2.218290924645696
(cloud, thunderstorms)    2.1602989776680093
(cloud, dissipated)    2.1602989776680093
(cloud, shear)    2.0877483105193977
(cloud, dust)    2.0302206014072985

TOP WORDS WITH LOVE

(love., lovers)    2.200433496898691
(love, madly)    2.1452809296045547
(love., refuses)    2.1035234838906347
(love,, love.)    2.0668152580904224 
(hates, love.)    2.0595746142964164
(love., loves)    2.053305933778326
(love., warns)    2.0369313651510303
(hurries, love)    2.031337577297718
(jealous, love.)    2.0243422378430096
(love., realizes)    2.0209090620046157    



------------------------
IN MAPPER IMPLEMENTATION
------------------------	

Answer 0. 
Pairs Approach : 

JOB 1:
The logic is mostly the same as above. The only difference is that here a new local HashMap is used in the mapper (1 Mapper would have 1 such Map). A Map of type Map<String, Integer> is initialized in the setup() through the getMap() method of the Mapper which stores all the word counts in the map() method. At the end for each map() a call to the flush() method checks if the buffer size is reached for the local map (buffer size = 200000). If yes then the contents of the map are written to context using the flush() method and map is cleared. The cleanup() method at then end also does a force flush to ensure that all key value pairs are written to context. There is no combiner reducer remains the same.

JOB 2:
Here again we a have a local Map of type Map<PairOfStrings, Double> which is one per Mapper. The setup() in the Mapper class initializes the map through the getMap() method. The map() method would count the co-occurences using the normal approach as described above (normal implementation) and put it in the local Map. At the end of the map() method, a call to the flush() method checks if the buffer size is reached for the local map. If yes then the contents of the map are written to context using the flush() method and the map is cleared. At the end the cleanup() method in the Mapper would forcefully flush all key value pairs to the context. There is no combiner reducer remains the same. 

Stripes Approach :

JOB 1: Same as the pairs approach.

JOB 2: 
Here again we a have a local Map of type Map<String, HMapStIW> which is one per Mapper. The setup() in the Mapper class initializes the map through the getMap() method. The map() method uses the logic of the previous (normal method explained above) approach of splitting the line and storing unique words in a TreeSet. The map() method stores unique co-occurences for all the lines in the local map. At the end of the map() method, a call to the flush() method checks if the buffer size is reached for the local map. If yes then the contents of the map are written to context using the flush() method and the map is cleared. At the end the cleanup() method in the Mapper would forcefully flush all key value pairs to the context. There is no combiner reducer remains the same.
    (Here the inner working uses the Stripes concept, however the output is kept in terms of pairs for easier analyzing of the outpt file)


-------------------------------
Running time and other stats
-------------------------------

For Bible Shakes
-------------------

Running time PairsPMI_InMapper (Job 1 -> 2.784 and Job 2 -> 34.394 seconds) On Local Mac Machine

JOB 1 --> Input Records -> 156215, Key Value Pairs -> 41788, Output -> 41788 reduce o/p rec
JOB 2 --> Input Records -> 156215, Key Value Pairs -> 2098417, Output -> 116759 reduce o/p rec

Running Time StripesPMI_InMapper ((Job 1 -> 2.776 and Job 2 -> 232.718 seconds) On Local Mac Machine

JOB 1 --> Input Records -> 156215, Key Value Pairs -> 41788, Output -> 41788 reduce o/p rec
JOB 2 --> Input Records -> 156215, Key Value Pairs -> 41788, Output -> 116966 reduce o/p rec


----------------------------
COMMANDS TO RUN - In Mapper
----------------------------

hadoop jar target/assignment2-1.0-SNAPSHOT-fatjar.jar edu.umd.nkher.PairsPMI_InMapper -input bible+shakes.nopunc -output PairsPMI_IM -numReducers 5

hadoop jar target/assignment2-1.0-SNAPSHOT-fatjar.jar edu.umd.nkher.StripesPMI_InMapper -input bible+shakes.nopunc -output StripesPMI_IM -numReducers 5
















