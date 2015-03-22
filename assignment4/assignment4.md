---------------------
Assignment 4 Answers
---------------------

Top Ten Nodes for Source 9470136
---------------------------------

Source: 9470136

0.38857 9470136

0.09418 7992850

0.08586 7891871

0.08063 10208640

0.06603 9427340

0.06603 8747858

0.03546 8702415

0.03183 8669492

0.02246 7970234

0.01693 8846238

Top Ten Nodes for Source 9300650
---------------------------------


Source: 9300650

0.44688 9300650

0.09108 10765057

0.08888 9074395

0.07597 10687744

0.07597 9621997

0.07597 8832646

0.01556 10448801

0.01511 9785148

0.01511 11890488

0.01511 10369305

Commands to run 
--------------------

<b>Step1-</b>
mvn clean package

<b>Step2-</b>
hadoop jar target/assignment4-1.0-SNAPSHOT-fatjar.jar edu.umd.nkher.BuildPersonalizedPageRankRecords -input sample-large.txt -output nameshkher-PageRankRecords -numNodes 1458 -sources 9470136,9300650

<b>Step3-</b>
hadoop fs -mkdir nameshkher-PageRank

<b>Step4-</b>
hadoop jar target/assignment4-1.0-SNAPSHOT-fatjar.jar edu.umd.nkher.PartitionGraph -input nameshkher-PageRankRecords -output nameshkher-PageRank/iter0000 -numPartitions 5 -numNodes 1458

<b>Step5-</b>
hadoop jar target/assignment4-1.0-SNAPSHOT-fatjar.jar edu.umd.nkher.RunPersonalizedPageRankBasic -base nameshkher-PageRank -numNodes 1458 -start 0 -end 20 -sources 9470136,9300650

<b>Step6-</b>
hadoop jar target/assignment4-1.0-SNAPSHOT-fatjar.jar edu.umd.nkher.ExtractTopPersonalizedPageRankNodes -input nameshkher-PageRank/iter0020 -top 10 -sources 9470136,9300650


