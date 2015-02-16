-------------------------------
Assignment 2 Answers
-------------------------------

---------------------------
For Bible Shakes
---------------------------

0. 
Pairs Approach : The implementation has two jobs. First jobs Mapper params <LongWritable, Text, Text, IntWritable>. This does a Word Count per sentence. Reducer params <Text, IntWritable, Text, IntWritable>. The output is stored in a file which is read by the next job. The second job's Mapper params are <LongWritable, Text, PairOfStrings, DoubleWritable>. Mapper calculates the unique cooccurences in each line and outputs the pair and one as count per line. The combiner params are <PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable> and does a key wise sum where the key is a pair of strings and sends to reducer. The Reducer params are <PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable>. The reducer in its setup() method reads the output of the first job and stores the word counts in a hash map (Loading Side data). The reduce function int he reducer calculates the total sum of pairs and then the pmi for them with the help of the hashmap created in the setup and emit's it!

Stripes Approach :  The implementation again has two jobs where. First jobs Mapper params <LongWritable, Text, Text, IntWritable>. This does a Word Count per sentence. Reducer params <Text, IntWritable, Text, IntWritable>. The output is stored in a file which is read by the next job. The second job's Mapper params are <LongWritable, Text, PairOfStrings, DoubleWritable>. Mapper takes params <LongWritable, Text, Text, HMapStIW> and calculates a hashmap for each word in a sentence. The hashmap cfor each word/text contains all other unique words it coccurs with and value as 1. 
For example if sentence is "Messi is a a football player" then it removes duplicates into a set <Messi is a football player> and then creates hashmap for each word eg for Messi the hashmap would be {is->1, a->, footballer->1, player->1}. The hashmap for the word "is" will not contain Messi as a key. Key values pairs in the form of (Text, HMapStIW) is passed to the combiners which does a sum and passed to the reducers. The reducer setup method reads the output of the first mapper (dictionary) and stores it in a hashmap. This hashmap is used for individual word count in the reduce() method of the reducer to calculate PMI!

1. With Combiner. Both run on the VM.
Running time Pairs (Job 1 -> 33.494 and Job 2 -> 72.479 seconds)
Running Time Stripes ((Job 1 -> 33.67 and Job 2 -> 82.718 seconds) 

JOB 1 --> Input Records -> 156215, Key Value Pairs -> 1531520, Output -> 41788 reduce o/p rec
JOB 2 --> Input Records -> 156215, Key Value Pairs -> 9985193, Output -> 116759 reduce o/p rec

2. Running without combiners. Both run on the VM.
Running time Pairs (Job 1 -> 44.882 and Job 2 -> 130.961 seconds)
Running Time Stripes (Job 1 -> 40.119 and Job 2 -> 91.5 seconds)

JOB 1 --> Input Records -> 156215, Key Value Pairs -> 1531520, Output -> 41788 reduce o/p rec
JOB 2 --> Input Records -> 156215, Key Value Pairs -> 9985193, Output -> 116759 reduce o/p rec

3.  99806 distinct pairs.

4. Pair with highest PMI value is (meshach, shadrach) and its PMI value is 4.047594697476721. Having a large PMI between these two terms signifies that when you see either word you know more about the probability of seeing the other word. Or seeing "meshach" gives you highest information about seeing "shadrach".  

5. TOP THREE WORDS WITH CLOUD

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


Note: FOT RUNNING ANY FILE (StripesPMI or PairsPMI) using this collection please change the number of lines to 114863.0. The variable is in the reducer code in both of the implementations.

0. Same as bible+shakes

1. With Combiner
Running time Pairs (Job 1 -> 73.051 Job 2 -> 11552.575 seconds) Run on Cluster. With 1 Reducer.

JOB 1 --> Input Records -> 114863, Key Value Pairs -> 11255671, Output -> 1064960 reduce o/p rec
JOB 2 --> Input Records -> 114863, Key Value Pairs -> 1910190399, Output -> 19105212 reduce o/p rec

Running Time Stripes (Job 1 -> 60.995 Job 2 -> 2845.837 seconds) Run on Cluster

JOB 1 --> Input Records -> 114863, Key Value Pairs -> 11255671, Output -> 1064960 reduce o/p rec
JOB 2 --> Input Records -> 114863, Key Value Pairs -> 11255671, Output -> 19105212 reduce o/p rec

2. Running without combiners. 

Running time Pairs (Job 1 ->  80.123 sec Job 2 -> 3124.23 seconds), Job 1 output -> 41788 reduce output records
Running Time Stripes (Job 1 ->  63.245 Job 2 -> 3433.23 seconds), Job 2 output -> 116759 reduce output records 

All other values are same as for the with combiner version.

3. 9414678 distinct pairs.

4. Get highest PMI -> (actor., would)    value -> 9.981732669697917E-5. 

5. TOP WORDS WITH CLOUD

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


------------------------------------
How to run the AnalyzePMI.java file 
------------------------------------

Note: The analyze files run on the sequence outputs of the bible+shakes file (both pair and stripes implementations). The sequence outputs are prerun and stored. They answer the last three questions.
	
mvn exec:java -Dexec.mainClass=edu.umd.nkher.AnalyzePMI \-Dexec.args="-input pairs_seq_op"

mvn exec:java -Dexec.mainClass=edu.umd.nkher.AnalyzePMI \-Dexec.args="-input stripes_seq_op"

------------------------------------
How to run the CountLines.java file 
------------------------------------

Note: This file prints the number of lines in a file, but the file should be in your project folder under assignment2.

mvn exec:java -Dexec.mainClass=edu.umd.nkher.CountLines \-Dexec.args="name_of_file"	
	













