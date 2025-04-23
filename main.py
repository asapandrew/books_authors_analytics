from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("Aythors and Books analytics").getOrCreate()

# Reading CSV files as DataFrame
df_authors = spark.read.csv("authors.csv", header=True, inferSchema=True, sep=",")
df_books = spark.read.csv("books.csv", header=True, inferSchema=True, sep=",")

# Data transformation
df_authors = df_authors.withColumn("birth_date", df_authors.birth_date.cast(DateType()))
df_books = df_books.withColumn("publish_date", df_books.publish_date.cast(DateType()))

# Data concatination
df = df_books.join(df_authors, "author_id", "left")

# Data Analytics
# Top-5 Authors with most revenue
df_most_revenue = df.groupBy("name").agg(sum("price").alias("total_revenue"))
df_most_revenue = df_most_revenue.orderBy(df_most_revenue.total_revenue.desc()).limit(5)

# Amount of books per genre
df_book_per_genre = df.groupBy("genre").agg(count("book_id").alias("books_count"))

# Average price by Author
df_avg_price = df.groupBy("name").agg(avg("price").alias("average_book_price"))

# Book published after 2000
df_book_after_2000 = df.filter(df["publish_date"] > "1999-12-31").orderBy(
    df.price.desc()
)
df_book_after_2000.show()

spark.stop()
