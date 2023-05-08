// spark-shell --deploy-mode client -i 

import org.apache.spark.sql.SparkSession

val df = spark.read.option("header", "true").option("delimiter", ",").option("multiLine",true).csv("project/original.csv"); 
// testing if columns are identified
df.select("Date").first().getString(0) ; 
df.count() ;     // 517866 lines
df.columns;

// drop unnecessary columns
    val dfDrop = df.drop(df.columns.slice(df.columns.indexOf("user"), df.columns.length):_*);
    val dfDrop2 = dfDrop.drop("Message-ID","To","From","X-Folder","X-Origin","X-FileName");
    dfDrop2.columns ;
    // _c0, Date, Subject, X-From, X-To, X-cc, X-bcc, content

// dropping duplicated rows
    val dfDup = dfDrop2.dropDuplicates().distinct();
    dfDup.count();  // 517827; about 39 duplicated rows removed

// combining columns cc and bcc
    val dfComb = dfDup.select(col("_c0"), col("Date"), col("Subject"), col("X-From"), col("X-To"),  
                                    concat_ws(";", col("X-cc"),col("X-bcc")).as("cc"),
                                    col("content"));
    dfComb.columns;
    // _c0, Date, Subject, X-From, X-To, cc, content

//filter empty content rows
    val dfFiltered = dfComb.filter(col("content").isNotNull)         // without content
                          .filter(col("_c0").cast("int").isNotNull) // remove misaligned rows
                          .filter(col("Date").isNotNull)            // remove rows without date
    dfFiltered.count();         // 514100

//-----------------------
// Date formatting, spliting year, month, day, and hour
val yearExp = "\\d{4}"
val monthExp = "/\\d{1,2}/"
val dayExp = "\\d{1,2}/"
val hourExp = "\\d{1,2}:"

val timeExp = "(\\d{4})-(\\d{2})-(\\d{2})(\\s{1})+(\\d{2}):(\\d{2}):(\\d{2})" ;

val dfTime = dfFiltered.select(col("_c0"),
    regexp_extract(col("Date"), timeExp, 1).alias("year"),
    regexp_extract(col("Date"), timeExp, 2).alias("month"),
    regexp_extract(col("Date"), timeExp, 3).alias("day"),
    regexp_extract(col("Date"), timeExp, 5).alias("hour"));

val dfDate = dfTime.join(dfFiltered, "_c0").filter(col("month").cast("int").isNotNull);
dfDate.columns      // _c0, year, month, day, hour, Date, Subject, X-From, X-To, X-cc, X-bcc, content

//-----------------------
// Text formatting
//replace email address with names
val dfText = dfDate.withColumn("X-To", regexp_replace(col("X-To"), "<.*?>", ""))
                    .withColumn("X-To", regexp_replace(col("X-To"), "@.*?\\.com", ""))
                    .withColumn("X-To", regexp_replace(col("X-To"), "\\.", " "));

// lowercase
val columnsLower = Seq("Subject", "X-From", "X-To", "cc", "content");
val dfLower = dfText.select(dfText.columns.map(x => if (columnsLower.contains(x)) lower(col(x)).alias(x) else col(x)): _*);


//-----------------------
dfLower.coalesce(1).write.option("header", "true").format("csv").save("hdfs://nyu-dataproc-m/user/hz2400_nyu_edu/project/sparkCleaned");

// hdfs dfs -rm -r project/cleaned/sparkV1.csv
// hdfs dfs -rm -r project/sparkCleaned

// hdfs dfs -ls project/sparkCleaned
// hdfs dfs -mv project/sparkCleaned/part-00000-4c43f000-6118-49b3-89e8-07412fb8d9a0-c000.csv project/cleaned/sparkV1.csv
// hdfs dfs -ls project/cleaned


