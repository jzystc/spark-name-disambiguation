# Name Disambiguation
This is a implementation of name disambiguation in citation databases based on Spark.
- Background
    
    In large-scale citation networks, there are many papers contains authors with same names. These names usually correspond to different people.
The name disambiguation task is to divide these papers into different clusters where papers in the same cluster belong to the same person and papers in the different clusters belong to different people.
## Algorithm
1. Build a graph where nodes represent papers and edges represent two nodes contain the same author name.
2. Extract raw features from papers. Features include title, abstract, author names, organizations, venue, publication year.
3. Transfer the title, abstract, orgnizations into vectors by Word2Vec
4. Train the logistic regression model
5. Classify edges. class 1 means two papers belong to the same author, class 0 means they belong to different authors

## Requirements
* Scala: 2.11.12 with jdk11 or 2.11.8 with jdk8
* Spark: 2.4.0 (2.3.x may be also compatible)
> Spark version needs to be compatible with Scala version. e.g., Spark 2.4.0 built with Scala 2.11.12

[comment]: <> (* mysql: 5.7)
## File Structure
* `network.AuthorNetwork`: implementation of building author network according the dataset 
* `util.DataPreparation`: implementation of loading data from the database and generating dataframe parquet files 
* `util.AnalysisTool`: implementation of evaluating the discrimination result of the method. 
* `util.Similarity`: implementation of computing text, org, coauthor and layer similarity between two nodes. 
* `NameDisambiguation`: main entry of the program

## How to run for 100 test names

[comment]: <> (1. run the `test.sql` in the project directory.)
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
