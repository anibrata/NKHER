ASSIGNMENT 5 ANSWERS
-------------------------------

1. What is the scalability issue with this particular HBase table design?

Answer: 

One of the scalability issues that can happen with this particular table design is when a document in the collection gets updated or modified. This means that new content is added to the document, some of the content is removed or even edited. This would cause us to do a full table scan in order to update the whole inverted index table. 

Currently a term in the collection acts as the row key which contains postings in the form of (docno, termFreq) pairs. Hence if a document in the collection is updated we would have to perform a table scan and update the corressponding termFreq for every term in the updated document.

2. How would you fix it? You don't need to actually implement the solution; just outline the design of both the indexer and retrieval engine that would overcome the scalability issues in the previous question.

Answer: 

Solution I:

For fixing the above problem we can maintain another table (list) which contains the reverse list. For example we could store the document_number as the row key with the words and their term frequencies as the column qualifier and cell values. The structure would then look something like the following:

{
  "docno": {
    "a": 1,
    "the": 2
  }
}

The above solution would prevent us from doing a complete table scan.

Solution II:

Alternatively, we can also modify the row key so that all the information is stored in the same table. For doing this the row key would be the combination of the document_number and the term frequency. 

Row Key -> docno + term

The column family hence can be shortened to say "f" which signifies that it only stores the frequency of the term. Shortening the column family would reduce the I/O load for both the disk and network. Now inorder to get a list of all the words in a particular document we would have to do a short SCAN instead of a GET operation. Also, deleting a word from a document would only be a simple delete operation from the table. We do not need to iterate over the entire row (posting list) as in the earlier designs. We can use MD5 hashing for getting fixed row key lengths.




COMMANDS TO RUN THE ASSIGNMENT
-------------------------------

FOR BIBLE SHAKES COLLECTION
----------------------------

hadoop jar target/assignment5-1.0-SNAPSHOT-fatjar.jar edu.umd.nkher.BuildInvertedIndexHBase -input bible+shakes.nopunc -output index-bibleshakes-nkher
   
hadoop jar target/assignment5-1.0-SNAPSHOT-fatjar.jar edu.umd.nkher.BooleanRetrievalHBase -collection bible+shakes.nopunc -index index-bibleshakes-nkher  

FOR WIKI COLLECTION
--------------------

hadoop jar target/assignment5-1.0-SNAPSHOT-fatjar.jar edu.umd.nkher.BuildInvertedIndexHBase -input /shared/simplewiki-20141222-pages-articles.txt -output index-simplewiki-nkher
   
hadoop jar target/assignment5-1.0-SNAPSHOT-fatjar.jar edu.umd.nkher.BooleanRetrievalHBase -collection /shared/simplewiki-20141222-pages-articles.txt -index index-simplewiki-nkher