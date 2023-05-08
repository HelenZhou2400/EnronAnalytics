// spark-shell --deploy-mode client -i 

import org.apache.spark.sql.SparkSession

val data = sc.textFile("project/original.csv");
// input.take(1)

val mapper =  data.map(line => {("lineCount", 1)});
val reducer = mapper.reduceByKey( (x,y) => x + y );
val results =reducer.collect();
results.foreach(println);       // (lineCount,537600)   that includes header

// however, map reduce would not correctly identify line count since some rows' content continued to a new line
// therefore, the file has 537600 lines, but that's not the correct number of records. 
// dataframe format allows counting multiple lines of one row's content
val df = spark.read.option("header", "true").option("delimiter", ",").option("multiLine",true).csv("project/original.csv"); 
df.count()      // 517866 lines

// or using map reduce method
val rowCount = df.rdd.map(_ => 1).reduce(_ + _);
println(s"True line count is $rowCount rows.");     //517866

// -----------------
// Structure and Content discovery
df.take(1);
df.head(2)(1);
df.head(3)(2);

// -----------------
// For some rows, every row is expected to have distinct values
// thus we check if there are duplicates that may need to be cleaned in later stages
df.printSchema();

// some examples:
val duplicates = df.groupBy("_c0").count.filter("count > 1");
duplicates.show();

val duplicates = df.groupBy("Message-ID").count.filter("count > 1");
duplicates.show();

val duplicates = df.groupBy("Date").count.filter("count > 1");
duplicates.show();

// -----------------
// find number of rows containing null data 
// someexamples
df.filter(col("_c0").isNull).count();    // 0 
df.filter(col("Date").isNull).count();    // 52 

df.filter(col("From").isNull).count();    // 443 
df.filter(col("To").isNull).count();    // 22478 

df.filter(col("X-From").isNull).count();    // 690 
df.filter(col("X-To").isNull).count();    // 9408 

df.filter(col("content").isNull).count();  // 3761

// complete null value count 
val nullCounts = df.columns.map(x =>count(when(isnan(col(x)) || col(x).isNull, x)).alias(x));
val nullTable = df.select(nullCounts:_*);

// nullTable.write.format("csv").save("hdfs://nyu-dataproc-m/user/hz2400_nyu_edu/project/null_profile");

