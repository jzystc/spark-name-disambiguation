# Name Disambiguation
This is a Spark GraphX implementation of name disambiguation in citation database.
## Requirements
   * scala: 2.11.12 (2.11.x)
   * spark: 2.4.0 (2.3.x)
   * mysql: 5.7
## File Structure
* `main.AuthorNetwork`: implementation of building author network according the dataset 
* `util.DataPreparation`: implementation of loading data from the database and generating dataframe parquet files 
* `util.AnalysisTool`: implementation of evaluating the discrimination result of the method. 
* `util.Similarity`: implementation of computing text, org, coauthor and layer similarity between two nodes. 
* `main.NameDisambiguation`: main entry of the program 
## How to run for 100 test names
1. run the `test.sql` in the project directory.
1. run the `NameDisambiguation.scala`,make sure the line with `analyze()` is uncommanded.

## How to run for a specified name
1. command the line with `analyze()`
1. uncommand the line with `analyzeByName(name)` 
1. change the value of `name` to the name you want to estimate.
  
## Use spark-submit to run on clusters
1. build `*.jar` first
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
