from pyspark import SparkContext, SparkConf
import sys
import math
from operator import add

class Project2:           
    def run(self, inputPath, outputPath, stopwords, topic, k):
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)

        # load txt file
        text = sc.textFile(inputPath)

        # Data Cleaning #

        # extract date as key and sentence
        date_and_sentence= text.map(lambda x: x.split(",", 1))

        # Map the year and list of tokens
        year_and_tokens = date_and_sentence.map(lambda parts: (
            parts[0][:4],                     # year = first 4 chars of date
            parts[1]                          # token
                .lower()                        # lowercase of each sentence
                .split()                        # split token at whitespaces
        ))

        # Raw term Frequency #

        # map all words to a dictionary
        all_words = year_and_tokens.flatMap(lambda x: x[1])

        # create value pairs with a count of each word
        raw_term_frequency = all_words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)

        # n stop words filtered
        n_stopwords = int(stopwords)
        
        # Figure out stop words.
        stopwords = (
        raw_term_frequency
            .sortBy(lambda x: (-x[1], x[0]))   # sort by count and then alphabetically 
            .keys()                            # get only the words
            .take(n_stopwords)                 # take the first K_stopwords words    
        )

        # Broadcast stop words
        stopwords_bcast = sc.broadcast(set(stopwords))

        # Filter out stop words from all words.
        year_terms = (
            year_and_tokens
            .mapValues(lambda tokens: set(tokens) - stopwords_bcast.value))

    
        # build the global document frequency of each term across all years
        #   i.e. in how many headlines it appears, across all years
        global_df = (
        year_terms
            .flatMap(lambda yr_terms: yr_terms[1])    # emit each term in the year as a separate record
            .map(lambda term: (term, 1))              # create a pair RDD with (term, 1) 
            .reduceByKey(lambda a, b: a + b)          # reduce to get global document frequency
        )

        # M values
        m = int(topic)

        # Pick the top-m terms by that global document frequency of each term
        top_m_terms = (
            global_df
            .sortBy(lambda term_df: (-term_df[1], term_df[0]))  # sort by DF desc, term asc
            .keys()                                             # get only the terms 
            .take(m)                                            # take top m terms
        )

        # Broadcast top m terms
        vocab_bcast = sc.broadcast(set(top_m_terms))

        # create document frequency for each word excluding stop words
        year_term_pairs = year_terms.flatMap(
        lambda yr_terms: [
            ((yr_terms[0], t), 1)
            for t in yr_terms[1]
            if t in vocab_bcast.value
        ]
    )
        
        # reduce to get document frequency by year and term
        #   year_term_pairs is a pair RDD with ((year, term), total_count)
        year_term_df = year_term_pairs.reduceByKey(add)

        # Top k terms by year 
        k = int(k)

        # Get top k terms for each year
        top_k_per_year = (
            year_term_df.map(
                lambda yt_count: (yt_count[0][0], (yt_count[0][1], yt_count[1])))   # ((year, term), count) -> (year, (term, count))
                .groupByKey()                                                       # group by year
                .mapValues(lambda term_counts: sorted(
                                term_counts,
                                key=lambda tc: (-tc[1], tc[0])                      # sort by DF desc, term asc
                            )
                        [:k]
                        )                                                           # take top k terms 
                    )

        # Sort by year for output
        top_k_per_year = top_k_per_year.sortByKey()

        # Turn the output into an RDD of lines
        lines_rdd = (
            top_k_per_year
            .sortByKey()                                                # ensure years come out in order
            .flatMap(lambda year_terms: 
                # year_terms is (year, [(term, df), …])
                [ f"{year_terms[0]}:{len(year_terms[1])}" ]             # first line: "YYYY:N"
                +
                [ f"{term}\t{df}" for term, df in year_terms[1] ]
            )
        )

        # coalesce to a single partition so you get one part-file
        single = lines_rdd.coalesce(1)

        # Write it out
        single.saveAsTextFile(outputPath)

        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])


