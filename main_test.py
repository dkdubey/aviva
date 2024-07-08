import pytest
from pyspark.sql import SparkSession
from your_module import (create_spark_session, load_data, clean_text, combine_text, 
                         split_and_filter_words, compute_word_frequencies, 
                         get_top_n_words, add_petition_id, transform_data)

@pytest.fixture(scope="module")
def spark():
    return create_spark_session()

@pytest.fixture
def sample_data():
    return [
        {"abstract": {"_value": "MPs should attend all debates, not merely turn up and vote or strike pairing deals. With other commitments, a five day Commons is not workable for MPs: I suggest three full days (9am to 6pm minimum), with one or two days for Committees, leaving at least one day for constituency work."}, "label": {"_value": "Reform the Commons: Three days full time with compulsory attendance for all MPs."}, "numberOfSignatures": 27},
        {"abstract": {"_value": "When you change your car you pay road tax for the whole month on your old car and on your new car, effectively paying for two cars when you own only one. Before recent changes to road tax discs and on line payments the tax was carried over to the new owner!"}, "label": {"_value": "Instruct the DVLA to charge road tax during the month of sale on a daily basis."}, "numberOfSignatures": 223},
    ]

def test_load_data(spark, sample_data):
    df = load_data(spark, sample_data)
    assert df.count() == 2
    assert set(df.columns) == {"abstract", "label", "numberOfSignatures"}

def test_clean_text(spark, sample_data):
    df = load_data(spark, sample_data)
    df_clean = clean_text(df)
    assert df_clean.filter(col("abstract").contains("MPs")).count() == 0

def test_combine_text(spark, sample_data):
    df = load_data(spark, sample_data)
    df_clean = clean_text(df)
    df_combined = combine_text(df_clean)
    assert "text" in df_combined.columns

def test_split_and_filter_words(spark, sample_data):
    df = load_data(spark, sample_data)
    df_clean = clean_text(df)
    df_combined = combine_text(df_clean)
    df_words = split_and_filter_words(df_combined)
    assert df_words.filter(length(col("word")) < 5).count() == 0

def test_compute_word_frequencies(spark, sample_data):
    df = load_data(spark, sample_data)
    df_clean = clean_text(df)
    df_combined = combine_text(df_clean)
    df_words = split_and_filter_words(df_combined)
    word_freq = compute_word_frequencies(df_words)
    assert word_freq.filter(col("count") > 1).count() > 0

def test_get_top_n_words(spark, sample_data):
    df = load_data(spark, sample_data)
    df_clean = clean_text(df)
    df_combined = combine_text(df_clean)
    df_words = split_and_filter_words(df_combined)
    word_freq = compute_word_frequencies(df_words)
    top_words = get_top_n_words(word_freq, 2)
    assert len(top_words) == 2

def test_add_petition_id(spark, sample_data):
    df = load_data(spark, sample_data)
    df_id = add_petition_id(df)
    assert "petition_id" in df_id.columns

def test_transform_data(spark, sample_data):
    df = load_data(spark, sample_data)
    df_clean = clean_text(df)
    df_combined = combine_text(df_clean)
    df_words = split_and_filter_words(df_combined)
    word_freq = compute_word_frequencies(df_words)
    top_words = get_top_n_words(word_freq, 2)
    df_id = add_petition_id(df_words)
    df_transformed = transform_data(df_id, top_words)
    assert df_transformed.count() > 0
    for word in top_words:
        assert word in df_transformed.columns
