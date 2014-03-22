HadoopWordCountRank
===================

Hadoop v 2.2 - program to find word count of specific words and rank web pages based on the count

A Hadoop application that takes as input the 50 Wikipedia web pages dedicated to the US 
states and outputs: 
a) How many times each of the words “education”, “politics”, “sports”, and 
“agriculture” appear in the files. 
b) Rank the top 3 states for each of these words (i.e., which state pages use each of 
these words the most) 


COMPILE AND PACK INTO JAR

set the java classpath to include the following:
export path HADOOP_HOME=/usr/local/hadoop (this is the hadoop home directory)
$HADOOP_HOME/share/hadoop/common/hadoop-common-2.2.0.jar
$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.2.0.jar
$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar
$HADOOP_HOME/share/hadoop/common/lib/log4j-1.2.17.jar

//compile
javac -d wcrank_classes *.java

//pack into jar
jar -cvf wordcountrank.jar -C wcrank_classes/ .

//Run hadoop wordcount and wordcount rank program - /states is the hdfs location containing the 50 wikipedia webpages
hadoop jar wordcountrank.jar WordCount /states /test/wordcountout
hadoop jar wordcountrank.jar WordCountRank /states /test/rankout

