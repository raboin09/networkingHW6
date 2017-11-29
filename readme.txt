**Log into the Zoidberg machine**

**Create the input and output directories if they don't already exist:**

hadoop fs -mkdir /user/jraboin/wordcount/output/
hadoop fs -mkdir /user/jraboin/wordcount/input/

**Compile and run the code (you can vary the number of reducers, for simplicity I just ran 1):**

export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
hadoop com.sun.tools.javac.Main *.java
jar cf bigram.jar *.class
hadoop jar bigram.jar BigramCount -input /user/ssalem/wordcount/input -numReducers 2 -output /user/ssalem/wordcount/output

**VIEW OUTPUT**

hadoop fs -ls /user/jraboin/wordcount/output/
hadoop fs -cat /user/jraboin/wordcount/output/part-r-00000
hadoop fs -rmdir /user/jraboin/wordcount/output
hadoop fs -rmr /user/jraboin/wordcount/output
