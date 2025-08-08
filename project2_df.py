from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import sys

class Project2:           
    def run(self, inputPath, outputPath, stopwords, topic, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        
        # sc for spark context
        sc = spark.sparkContext

        # this line suppresses INFO & WARN logs
        sc.setLogLevel("ERROR") 

        # Data Cleaning 
        df = (
            spark.read.text(inputPath)
            .withColumn("parts",    F.split(F.col("value"), ",", 2))        # split 'value' on the first two commas [date, sentence]        
            .withColumn("date",     F.col("parts").getItem(0))              # first element of the array as a new 'date' column
            .withColumn("headline", F.col("parts").getItem(1))              # second element as headline
            .withColumn("year",     F.substring("date", 1, 4))              # extract the year from the date string
            .drop("value","parts")                                          # drop the 'value' and 'parts' column
        )
    
        # Tokenise into an array of lower-case words and add to data frame
        df = df.withColumn(
            "tokens",
            F.split(F.lower(F.col("headline")), r"\s+")     # split headline column at the blank spaces
        )

        # Dataframe for terms
        raw_tf = (df
            .select(F.explode("tokens").alias("term"))     # f.explode() to create a new row for each token
            .groupBy("term").count()
        )

        # number of stop words to filter
        n_stopwords = int(stopwords)

        # Figure out stop words.
        stopwords = [row.term for row in            
            raw_tf
            .orderBy(F.desc("count"), "term")                                 # sort by count and then alphabetically
            .limit(n_stopwords)                                               # take the first K_stopwords words
            .collect()                                                        # collect as a list
        ]

        # Add a unique doc_id to each row
        df = df.withColumn("doc_id", F.monotonically_increasing_id())
        
        # explode tokens into separate rows and filter out stop words
        exploded = (df
            .select("doc_id","year", F.explode("tokens").alias("term"))         # explode tokens into separate rows
            .where(~F.col("term").isin(stopwords))                              # filter out stop words
            .dropDuplicates(["doc_id","term"])                                  # remove duplicates
        )

        # Global document-frequency → pick top-m topic terms
        global_df = (exploded                                                   # exploded dataframe
            .groupBy("term")                                                    # group by term         
            .agg(F.countDistinct("doc_id").alias("df"))                         # count distinct doc_id for each term      
        )
        
        # Number of top terms to consider
        m = int(topic)
        
        # Top m terms across all years
        top_m = [row.term for row in
            global_df
            .orderBy(F.desc("df"), "term")                                    # sort by df and then alphabetically
            .limit(m)                                                         # take the first m terms                
            .collect()                                                        # collect as a list 
        ]

        # Filter to just those m terms, then per-year DF
        year_df = (exploded
            .where(F.col("term").isin(top_m))                                   # filter to just those m terms        
            .groupBy("year","term")                                             # group by year and term    
            .agg(F.countDistinct("doc_id").alias("df"))                         # count distinct doc_id for each term in each year
        )

        # Rank within each year and keep top-k
        k = int(k)

        # Rank terms within each year and keep top-k
        w = Window.partitionBy("year").orderBy(F.desc("df"), F.asc("term"))
        
        # Rank and filter to top-k terms per year
        ranked = (year_df
            .withColumn("rank", F.row_number().over(w))                         # rank terms within each year by df and then alphabetically  
            .where(F.col("rank") <= k)                                          # filter to top-k terms
            .drop("rank")                                                       # drop the rank column
        )

        # Collect & write exactly the same format
        out_rows = (ranked
        .orderBy("year", F.desc("df"), "term")
        .collect()
        )


        # order the ranked DF so collect_list preserves your sort
        ordered = ranked.orderBy("year", F.desc("df"), F.asc("term"))

        # group into per-year buckets
        grouped = ordered.groupBy("year").agg(
            F.collect_list(F.struct("term","df")).alias("bucket")
        )

        # sort the grouped df by year ascending
        sorted_grouped = grouped.orderBy(F.col("year").cast("int"))
        
        # explode into lines and save
        lines_rdd = (
            sorted_grouped.rdd
                .flatMap(lambda row: [f"{row.year}:{len(row.bucket)}"]
                                        + [f"{t.term}\t{t.df}" for t in row.bucket])
        )

        lines_rdd.coalesce(1).saveAsTextFile(outputPath)
                        
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

