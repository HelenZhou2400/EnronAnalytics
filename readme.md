## Readme File - Code Documentation

#### **Introduction**
This repository aims to analyze the Enron Email Dataset from: data.world/brianray/enron-email-dataset. 


#### **HDFS and Spark Scala**
All the codes for this project are coded in Spark scala. The input and output are both stored in HDFS platform. Reproducing the output would require changing certain files' paths. 

The files are named Profiling.scala, Cleaning.scala, and Analytics.scala. Each correspond to one stage of project. 
* The output of profiling will print on screen after running the scala file in spark-shell.
* The output of cleaning is a csv stored in HDFS project/cleaned/sparkV1.csv. The original output is stored as project/sparkCleaned/part-00000-random_output_sequence.csv but then renamed and moved for easier access in later stages.
* The output of analystics are stored in project/Analytics/wordFreq.csv and sentiment analysis result will output on screen when scala file runs. Similarly, the original output are renamed and moved for easier access for visualization. 

Screenshots of outputs are stored in the screenshots_output folder with subfolder named after each stages. 



##### Profiling stage
In the profiling stage, the number of lines in the csv file is counted using MapReduce method. However, because some records takes multiple lines, the accurate number of records are calculated through reading csv as a dataframe. 
Some more reserach on structure and content discovery are done in this step. 
When running the Profiling.scala, the output is printed on screen as well as commented inline

##### Cleaning stage
For cleaning part, duplicates and irrelevant columns are removed. Email cc name list was merged with bcc list.  Date is formatted into four different fields: year, month, day, and hour. email addresses inside recipient field is cleaned, excessive domain names and symbols are removed. Text inside the each column are formated to lowercase using mapping method.  
Afterall, the cleaned version of data is stored in HDFS located as described in the first section. To reproduce the output, one has to remove the existing csv files and directories as follows (changing path accordingly) : 
**removing duplicates:**
    hdfs dfs -rm -r project/cleaned/sparkV1.csv
    hdfs dfs -rm -r project/sparkCleaned
**renaming and moving output**
    hdfs dfs -ls project/sparkCleaned
    hdfs dfs -mv project/sparkCleaned/part-00000-4c43f000-6118-49b3-89e8-07412fb8d9a0-c000.csv project/cleaned/sparkV1.csv
    hdfs dfs -ls project/cleaned

##### Analytics stage
Eamil volume distribution over the years is printed on-screen and later visualized using Google Sheets. Frequencies of word used in emails are counted and output to a csv file project/Analytics/wordFreq.csv. A pre-trained model from spark-nlp is used for sentiment analysis. The sentiment results are counted and filtered by year. The output of sentiment analysis will print on screen when scala file runs. The count of sentiment results are copied from on screen to seperate csv file. These csv files are stored in visualization subfolder. Using the cloudGraph.py, one can generate word cloud based on input csv. 

##### Notes
A full publication of this project's result is on medium: https://medium.com/@hz2400/analysis-on-corporation-email-communications-5074b8077e60



