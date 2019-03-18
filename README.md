# spark-name-disambiguation
> ## Introduction
> * Dependencies
>   > scala: 2.11.12 (support scala 2.11.x)\
>   > spark: 2.4.0 (support spark 2.3.x)\
>   > mysql: 5.7
> * File Structure
>   > `/src/main/scala/main/AuthorNetwork.scala` \
&emsp; implementation of building author network according the dataset \
>   > `/src/main/scala/util/DataPreparation.scala` \
&emsp; implementation of loading data from the database and generating dataframe parquet files \
>   > `/src/main/scala/util/AnalysisTool.scala` \
&emsp; implementation of evaluating the discrimination result of the method. \
>   >`/src/main/scala/util/Similarity.scala` \
&emsp; implementation of computing text, org, coauthor and layer similarity between two nodes. \
>   >`/src/main/scala/main/NameDisambiguation.scala` \
&emsp; main entry of the program 
## How to run for 100 test names
1. run the `test.sql` in the project directory.
1. run the `NameDisambiguation.scala`,make sure the line with `analyze()` is uncommanded.

## How to run for a specified name
1. command the line with `analyze()`
1. uncommand the line with `analyzeByName(name)` 
1. change the value of `name` to the name you want to estimate.
  
## Use spark-submit to run on clusters
1. build jar first
2. upload the jar to the driver node
3. run the command in the `spark-submit.txt`, you can modify the command to adapt to your cluter
```
spark-submit --executor-cores 2 \
    --total-executor-cores 180 \
    --executor-memory 4G \
    --driver-memory 100G \
    --conf spark.default.parallelism=300 \
    --master ```spark://DriverNodeIp:17077 \
    --class entity.AuthorNetwork /path/to/jar/filename.jar \
    0.6 0.5 0.35 \
