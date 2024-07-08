from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, regexp_replace, row_number, count, length
from pyspark.sql.window import Window

def create_spark_session():
    return SparkSession.builder \
        .appName("PetitionProcessing") \
        .getOrCreate()

def load_data(spark, data):
    return spark.createDataFrame(data)

def clean_text(df):
    return df.withColumn("abstract", lower(col("abstract._value"))) \
             .withColumn("label", lower(col("label._value"))) \
             .withColumn("abstract", regexp_replace(col("abstract"), "[^a-zA-Z\\s]", "")) \
             .withColumn("label", regexp_replace(col("label"), "[^a-zA-Z\\s]", ""))

def combine_text(df):
    return df.withColumn("text", col("abstract") + " " + col("label"))

def split_and_filter_words(df):
    return df.withColumn("word", explode(split(col("text"), "\\s+"))) \
             .withColumn("word", col("word")).filter(length(col("word")) >= 5)

def compute_word_frequencies(df):
    return df.groupBy("word").count().orderBy(col("count").desc())

def get_top_n_words(df, n=20):
    return [row["word"] for row in df.limit(n).collect()]

def add_petition_id(df):
    window = Window.orderBy("numberOfSignatures")
    return df.withColumn("petition_id", row_number().over(window))

def transform_data(df, top_words):
    return df.groupBy("petition_id") \
        .pivot("word", top_words) \
        .agg(count("word")) \
        .na.fill(0)

def save_to_csv(df, path):
    df.write.csv(path, header=True)

def main():
    spark = create_spark_session()

    # Sample data based on the provided structure
    data = [
        # ... (include the sample data here)
    ]

    df = load_data(spark, data)
    df = clean_text(df)
    df = combine_text(df)
    df = split_and_filter_words(df)
    word_freq = compute_word_frequencies(df)
    top_20_words = get_top_n_words(word_freq, 20)
    df = add_petition_id(df)
    df_pivot = transform_data(df, top_20_words)
    save_to_csv(df_pivot, "output_petitions.csv")

    spark.stop()

if __name__ == "__main__":
    main()
