
# Top-K Topic Terms by Year with PySpark

This project extracts the **top-k most important terms per year** from a large set of headlines using PySpark. It combines **text cleaning**, **stopword removal**, **document frequency analysis**, and **ranking** within each year to identify trends and keywords.

---

## 📌 Problem Statement

Given a dataset of dated headlines, extract the top-k topic terms per year based on document frequency (DF), after filtering out common stopwords and noise.

---

## 🧠 Key Features

- Text preprocessing and tokenisation
- Stopword detection based on top-n most frequent terms
- Global document frequency calculation
- Per-year ranking of top-k topic terms
- Clean, efficient PySpark implementation with RDD and DataFrame APIs

---

## 🛠️ Technologies Used

- Python 3.x
- PySpark (DataFrame + Window functions)
- Functional programming (`explode`, `collect_list`, etc.)

---

## 📁 Input Format

Each line of the input file should be in the format:

```
YYYY-MM-DD,headline text here
```

Example:
```
2007-03-14,Bank shares rally after rate decision
```

---

## 🚀 How It Works

1. **Read and parse input** into `date`, `year`, and `headline`
2. **Tokenise** and lowercase the text
3. **Identify stopwords** as the top-N most frequent terms
4. **Remove stopwords** and deduplicate (doc_id, term) pairs
5. **Compute global document frequency (DF)** for each term
6. **Select top-M topic terms** with highest global DF
7. **Compute per-year DF** for these terms
8. **Rank and filter top-K terms per year**
9. **Output results** in the format:
    ```
    2005:3
    economy 5
    market 4
    crisis 3
    ```

---

## 💻 Usage

### CLI

```bash
spark-submit project2.py <inputPath> <outputPath> <num_stopwords> <num_topics> <k>
```

### Parameters

- `<inputPath>`: Path to the input text file
- `<outputPath>`: Directory where results will be saved
- `<num_stopwords>`: Number of stopwords to ignore
- `<num_topics>`: Number of global topic terms to track
- `<k>`: Top-K terms to keep per year

---

## 📦 Example

```bash
spark-submit project2.py data/headlines.txt output/ 50 200 10
```

This command:
- Removes the top 50 most common words
- Selects the top 200 topic terms
- Outputs the top 10 terms per year based on DF

---

## 🧾 Output Format

Text output file will contain one section per year:

```
2006:3
inflation    42
budget       39
growth       38

2007:3
stocks       48
banking      45
economy      44
```

---

## 👨‍💻 Author

JJ Takhar  


---

## 📝 License

This project is licensed under the MIT License.
