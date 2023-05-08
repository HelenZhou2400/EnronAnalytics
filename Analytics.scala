// spark-shell --packages com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.0 --deploy-mode client -i 

import org.apache.spark.sql.SparkSession

val df = spark.read.option("header", "true").option("delimiter", ",").option("multiLine",true).csv("project/cleaned/sparkV1.csv"); 

df.columns;
df.count()

// word count of content
    val wordCountDf = df.withColumn("word_count", size(split(col("content"), "\\s+")))

    val meanValue = wordCountDf.select(mean(col("word_count"))); 
    meanValue.show() ;       // about 135 words per record

    val maxValue = wordCountDf.select(max(col("word_count"))); 
    maxValue.show() ;        // max with 45464 words

    val minValue = wordCountDf.select(min(col("word_count"))); 
    minValue.show() ;        // min with 1 words as we removed all empty content


// ---------------------------------
// Binary representaion 
val dfBinary = df.withColumn("work", when(col("hour").cast("int").between(9, 17), 1).otherwise(0));

// email during work time
val binaryCounts = dfBinary.groupBy("work").count();
binaryCounts.show();

// filtering based on date 
df.groupBy("year").count().show();
df.groupBy("month").count().show();
df.groupBy("day").count().show();

// email sent distribution
df.groupBy("year","month").count().show(200);


// the first and last email sent time
val earliestTime = df.groupBy("year", "month", "day").agg(min("hour").as("hour")).orderBy(col("year").asc, col("month").asc, col("day").asc, col("hour").asc);
earliestTime.show(20);
val latestTime = df.groupBy("year", "month", "day").agg(max("hour").as("hour")).orderBy(col("year").desc, col("month").desc, col("day").desc, col("hour").desc);
latestTime.show(20);

// ---------------------------------
// email word frequency count
// without removing stop words
val tokenized = df.select(split(col("content"), "\\s+").alias("words"));
val words = tokenized.select(explode(col("words")).alias("word"));
val wordCounts = words.groupBy("word").count().orderBy(desc("count"));
wordCounts.show(20);     // meaningless result

// removing stop words
import org.apache.spark.ml.feature.StopWordsRemover;
val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered");
val dfRemoved = remover.transform(tokenized)
val words = dfRemoved.select(explode(col("filtered")).alias("word"));
val wordCounts = words.groupBy("word").count().orderBy(desc("count"))
wordCounts.show(20);        // meaningless as many results are symbols

// removing symbols, words too short, and stop words
def cleaning(s: String): String = {
  val clean = s.replaceAll("[.@&]", " ").replaceAll("[^a-zA-Z ]", "").replaceAll("\\b\\w{1,4}\\b", "")
  clean.split("\\s+").filter(_.length > 4).mkString(" ")
}
val df2 = df.withColumn("content_sym", udf(cleaning _).apply(col("content")));

val tokenized2 = df2.select(split(col("content_sym"), " ").alias("words"));
val dfCleaned = remover.transform(tokenized2)

val words2 = dfCleaned.select(explode(col("filtered")).alias("word"));
val wordCountsAll = words2.groupBy("word").count().orderBy(desc("count"));
wordCountsAll.show(40);  // readable, all word frequencies

// for 1999 word frequencies
val df_left = df2.withColumn("id1",monotonicallyIncreasingId);
val df_right = words2.withColumn("id2",monotonicallyIncreasingId);

val df3 = df_left.join(df_right,col("id1")===col("id2"),"inner").drop("id1","id2")
val wordCounts1999 = df3.filter(col("year")==="1999").groupBy("word").count().orderBy(desc("count")); 
wordCounts1999.show(40); 

// for 2000 word frequencies
val wordCounts2000 = df3.filter(col("year")==="2000").groupBy("word").count().orderBy(desc("count")); 
wordCounts2000.show(100); 

//2001
val wordCounts2001 = df3.filter(col("year")==="2001").groupBy("word").count().orderBy(desc("count")); 
wordCounts2001.show(100); 

//2002
val wordCounts2002 = df3.filter(col("year")==="2002").groupBy("word").count().orderBy(desc("count")); 
wordCounts2002.show(40); 

wordCounts2.coalesce(1).write.option("header", "true").format("csv").save("hdfs://nyu-dataproc-m/user/hz2400_nyu_edu/project/wordFreq");


// ---------------------------------
// sentimental analysis using pre-trained model

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline;
val pipeline = new PretrainedPipeline("analyze_sentiment", lang = "en");


val sentences = df.select(col("content"));
val sentences1999 = df.select(col("content")).filter(col("year") === "1999");
val sentences2000 = df.select(col("content")).filter(col("year") === "2000");
val sentences2001 = df.select(col("content")).filter(col("year") === "2001");
val sentences2002 = df.select(col("content")).filter(col("year") === "2002");


def sentiment(s: String): String = {
  val result = pipeline.annotate(s);
  result("sentiment")(0);
}
val sentimentUDF = udf(sentiment _)

val res = sentences.withColumn("sentiment", sentimentUDF($"content"));  
// res.show()

val res1999 = sentences1999.withColumn("sentiment", sentimentUDF($"content"));    
val res2000 = sentences2000.withColumn("sentiment", sentimentUDF($"content"));    
val res2001 = sentences2001.withColumn("sentiment", sentimentUDF($"content"));    
val res2002 = sentences2002.withColumn("sentiment", sentimentUDF($"content"));    

res.groupBy("sentiment").count().show();      // positive 186102; negative 302897; na 24715
res1999.groupBy("sentiment").count().show();  // positive 3846; negative 6807; na 446
res2000.groupBy("sentiment").count().show();  // positive ; negative ; na 
res2001.groupBy("sentiment").count().show();  // positive 100022; negative 159064; na 11938
res2002.groupBy("sentiment").count().show();  // positive 14506; negative 20451; na 789

val sentences2001 = df.select(col("content"),col("month")).filter(col("year") === "2001");
val res2001 = sentences2001.withColumn("sentiment", sentimentUDF($"content"));    
res2001.groupBy("sentiment","month").count().show(40);  // positive 100022; negative 159064; na 11938
