from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, regexp_replace, row_number, count, length
from pyspark.sql.window import Window

# Sample data based on the provided structure
data = [
    {"abstract": {"_value": "MPs should attend all debates, not merely turn up and vote or strike pairing deals. With other commitments, a five day Commons is not workable for MPs: I suggest three full days (9am to 6pm minimum), with one or two days for Committees, leaving at least one day for constituency work."}, "label": {"_value": "Reform the Commons: Three days full time with compulsory attendance for all MPs."}, "numberOfSignatures": 27},
    {"abstract": {"_value": "When you change your car you pay road tax for the whole month on your old car and on your new car, effectively paying for two cars when you own only one. Before recent changes to road tax discs and on line payments the tax was carried over to the new owner!"}, "label": {"_value": "Instruct the DVLA to charge road tax during the month of sale on a daily basis."}, "numberOfSignatures": 223},
    {"abstract": {"_value": "CofE attendance is 765,000, representing 1.4% of England's population. The House of Lords has 26 CofE Bishops who participate in debates and vote in divisions, which involve decisions affecting the entire UK. This is wholly disproportionate to the size of the CofE's attendance and thus influence."}, "label": {"_value": "Disestablish the Church of England and establish Britain as a secular state"}, "numberOfSignatures": 176},
    {"abstract": {"_value": "Once the Leeming to Barton stretch of the A1(M) is completed, the route from Aberford to Birtley, near Gateshead, should be renamed as the M1. This would boost economic growth in the North East and North Yorkshire and would be easier for foreign drivers as their is no longer a sudden number change."}, "label": {"_value": "Once completed, Renumber the A1(M) North of Aberford as the M1."}, "numberOfSignatures": 15},
    {"abstract": {"_value": "People only get assistance with care home fees if their assets fall below the minimum threshold which is very low anyway and also if they need nursing care as opposed to residential care. Fees should be free for all."}, "label": {"_value": "Make care home fees free for all and not just free for people with low assets."}, "numberOfSignatures": 12},
    {"abstract": {"_value": "We read about the poor conditions that some animals live in and want to end the cruelty. Chickens are a start to achieving our goal because the effects gained from this would be most beneficial."}, "label": {"_value": "Stop all chain supermarkets selling eggs from battery and barn hens by 2018"}, "numberOfSignatures": 31},
    {"abstract": {"_value": "Transgender people in the UK are forced to pay to prove their identity to a Gender Recognition Panel. This process is humiliating, outdated and unnecessary. We urge the government to introduce an act equivalent to the Irish Gender Recognition Act, and allow trans* people to self-define their gender."}, "label": {"_value": "Allow transgender people to self-define their legal gender"}, "numberOfSignatures": 26},
    {"abstract": {"_value": "The two most effective NHS oral tablets to prevent Multiple Sclerosis relapses without the need for regular hospital stays for injected treatment are made of beef gelatin, so cannot be taken by many. With 100,000 MS sufferers in the UK and, benefits being cut, people's health needs to be preserved."}, "label": {"_value": "Make the most effective oral NHS tablets for Multiple Sclerosis animal fat-free"}, "numberOfSignatures": 65},
    {"abstract": {"_value": "I feel that many now, so far on from it's inception truly understand a lot about when and how it started. As a whole, we just take it for granted, and have become blase about it. Yet it's treated all of us, saved lives, including my own, and more than once. It deserves to be celebrated by us all."}, "label": {"_value": "Make the 5th of July NHS day, and a national holiday."}, "numberOfSignatures": 181},
    {"abstract": {"_value": "I’m calling for this to be reviewed, contraception is free on the NHS, but a person with a mental disability (depression) is to be charged for having what can’t be prevented. For people who work, prescriptions are chargeable, assuming people can afford it. This is not the case."}, "label": {"_value": "Anti depressants should be free to everyone who suffers depression."}, "numberOfSignatures": 13},
    {"abstract": {"_value": "The university students are unable to pay for their prescriptions as they can't work because of the full time education."}, "label": {"_value": "Free NHS prescriptions for full time students."}, "numberOfSignatures": 56},
    {"abstract": {"_value": "This year the government decided to change the year 2 SATs tests to tests far beyond what most children of that age can achieve. Teachers were trusted to make judgement on the children they taught. This year that has gone out the window too. Help save childrens education and enjoyment of school."}, "label": {"_value": "Stop KS1 tests and go back to teacher judgements and levels"}, "numberOfSignatures": 29463},
    {"abstract": {"_value": "Presently several politicians claim the lack of alcohol-licensing at airports is to blame for an increase in air rage incidents.  Therefore several are pushing to restrict in-flight alcohol-sales at airports and in-flight, which would punish the majority."}, "label": {"_value": "Introduce a 'no-fly' list banning disruptive & dangerous passengers from flying."}, "numberOfSignatures": 21},
    {"abstract": {"_value": "To withdraw the literacy and numeracy tests for PGCE applications. We are losing talented trainee teachers because of these QTS tests. The government requires you to wait two years to re-sit these tests so in effect your career is deferred for two years regardless of your teaching practice."}, "label": {"_value": "Remove the current procedure for the PGCE Skills test - Literacy and Numeracy"}, "numberOfSignatures": 43},
    {"abstract": {"_value": "To give working families the choice of whether they want the 30hrs chilcare funding from the age of 3 in 2017 or if they want to split it so they get 15hrs from the age of 2 and 15hrs from the age of 3 has this would benefit alot of families more then the 30hrs from the age of 3."}, "label": {"_value": "Working families choice of 30hr funding at age 3 or 15hrs at 3 and 15 at 2"}, "numberOfSignatures": 58},
    {"abstract": {"_value": "The government is not doing enough to combat mental health issues in NHS staff, and we believe that more support should be in place for workers who are suffering, to avoid tragedies like the ones we have already seen in the future."}, "label": {"_value": "Provide better mental health training and support for NHS staff"}, "numberOfSignatures": 221},
    {"abstract": {"_value": "The treason laws have not been updated for years. We believe that joining ISIS, or any other terrorist organisation which is an enemy of our country, should be classed as an act of treason. "}, "label": {"_value": "Update the treason laws to deal with British terrorists"}, "numberOfSignatures": 526},
    {"abstract": {"_value": "The Anglican Church insists that gay clergy with partners live a celibate life if they wish to continue working as priests or bishops. This is an unfair burden on gay clergy as well as being discriminatory within the 21st century workplace."}, "label": {"_value": "Make it illegal for the C of E to discriminate against gay non-celibate clergy"}, "numberOfSignatures": 1202},
    {"abstract": {"_value": "We, the undersigned, would like to request that a second referendum on the United Kingdom's membership of the EU should be held, due to the indication that demographics are now likely to have changed across the UK and many voters regret their decision on June 23rd."}, "label": {"_value": "Hold a second EU referendum on the grounds that demographics have now changed."}, "numberOfSignatures": 905}
]

def create_spark_session():
    return SparkSession.builder \
        .appName("PetitionProcessing") \
        .getOrCreate()

def load_data(spark, data):
    #peopleDF = spark.read.json(path) if path is provided
    peopleDF = spark.createDataFrame(data)
    return peopleDF

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
    {"abstract": {"_value": "MPs should attend all debates, not merely turn up and vote or strike pairing deals. With other commitments, a five day Commons is not workable for MPs: I suggest three full days (9am to 6pm minimum), with one or two days for Committees, leaving at least one day for constituency work."}, "label": {"_value": "Reform the Commons: Three days full time with compulsory attendance for all MPs."}, "numberOfSignatures": 27},
    {"abstract": {"_value": "When you change your car you pay road tax for the whole month on your old car and on your new car, effectively paying for two cars when you own only one. Before recent changes to road tax discs and on line payments the tax was carried over to the new owner!"}, "label": {"_value": "Instruct the DVLA to charge road tax during the month of sale on a daily basis."}, "numberOfSignatures": 223},
    {"abstract": {"_value": "CofE attendance is 765,000, representing 1.4% of England's population. The House of Lords has 26 CofE Bishops who participate in debates and vote in divisions, which involve decisions affecting the entire UK. This is wholly disproportionate to the size of the CofE's attendance and thus influence."}, "label": {"_value": "Disestablish the Church of England and establish Britain as a secular state"}, "numberOfSignatures": 176},
    {"abstract": {"_value": "Once the Leeming to Barton stretch of the A1(M) is completed, the route from Aberford to Birtley, near Gateshead, should be renamed as the M1. This would boost economic growth in the North East and North Yorkshire and would be easier for foreign drivers as their is no longer a sudden number change."}, "label": {"_value": "Once completed, Renumber the A1(M) North of Aberford as the M1."}, "numberOfSignatures": 15},
    {"abstract": {"_value": "People only get assistance with care home fees if their assets fall below the minimum threshold which is very low anyway and also if they need nursing care as opposed to residential care. Fees should be free for all."}, "label": {"_value": "Make care home fees free for all and not just free for people with low assets."}, "numberOfSignatures": 12},
    {"abstract": {"_value": "We read about the poor conditions that some animals live in and want to end the cruelty. Chickens are a start to achieving our goal because the effects gained from this would be most beneficial."}, "label": {"_value": "Stop all chain supermarkets selling eggs from battery and barn hens by 2018"}, "numberOfSignatures": 31},
    {"abstract": {"_value": "Transgender people in the UK are forced to pay to prove their identity to a Gender Recognition Panel. This process is humiliating, outdated and unnecessary. We urge the government to introduce an act equivalent to the Irish Gender Recognition Act, and allow trans* people to self-define their gender."}, "label": {"_value": "Allow transgender people to self-define their legal gender"}, "numberOfSignatures": 26},
    {"abstract": {"_value": "The two most effective NHS oral tablets to prevent Multiple Sclerosis relapses without the need for regular hospital stays for injected treatment are made of beef gelatin, so cannot be taken by many. With 100,000 MS sufferers in the UK and, benefits being cut, people's health needs to be preserved."}, "label": {"_value": "Make the most effective oral NHS tablets for Multiple Sclerosis animal fat-free"}, "numberOfSignatures": 65},
    {"abstract": {"_value": "I feel that many now, so far on from it's inception truly understand a lot about when and how it started. As a whole, we just take it for granted, and have become blase about it. Yet it's treated all of us, saved lives, including my own, and more than once. It deserves to be celebrated by us all."}, "label": {"_value": "Make the 5th of July NHS day, and a national holiday."}, "numberOfSignatures": 181},
    {"abstract": {"_value": "I’m calling for this to be reviewed, contraception is free on the NHS, but a person with a mental disability (depression) is to be charged for having what can’t be prevented. For people who work, prescriptions are chargeable, assuming people can afford it. This is not the case."}, "label": {"_value": "Anti depressants should be free to everyone who suffers depression."}, "numberOfSignatures": 13},
    {"abstract": {"_value": "The university students are unable to pay for their prescriptions as they can't work because of the full time education."}, "label": {"_value": "Free NHS prescriptions for full time students."}, "numberOfSignatures": 56},
    {"abstract": {"_value": "This year the government decided to change the year 2 SATs tests to tests far beyond what most children of that age can achieve. Teachers were trusted to make judgement on the children they taught. This year that has gone out the window too. Help save childrens education and enjoyment of school."}, "label": {"_value": "Stop KS1 tests and go back to teacher judgements and levels"}, "numberOfSignatures": 29463},
    {"abstract": {"_value": "Presently several politicians claim the lack of alcohol-licensing at airports is to blame for an increase in air rage incidents.  Therefore several are pushing to restrict in-flight alcohol-sales at airports and in-flight, which would punish the majority."}, "label": {"_value": "Introduce a 'no-fly' list banning disruptive & dangerous passengers from flying."}, "numberOfSignatures": 21},
    {"abstract": {"_value": "To withdraw the literacy and numeracy tests for PGCE applications. We are losing talented trainee teachers because of these QTS tests. The government requires you to wait two years to re-sit these tests so in effect your career is deferred for two years regardless of your teaching practice."}, "label": {"_value": "Remove the current procedure for the PGCE Skills test - Literacy and Numeracy"}, "numberOfSignatures": 43},
    {"abstract": {"_value": "To give working families the choice of whether they want the 30hrs chilcare funding from the age of 3 in 2017 or if they want to split it so they get 15hrs from the age of 2 and 15hrs from the age of 3 has this would benefit alot of families more then the 30hrs from the age of 3."}, "label": {"_value": "Working families choice of 30hr funding at age 3 or 15hrs at 3 and 15 at 2"}, "numberOfSignatures": 58},
    {"abstract": {"_value": "The government is not doing enough to combat mental health issues in NHS staff, and we believe that more support should be in place for workers who are suffering, to avoid tragedies like the ones we have already seen in the future."}, "label": {"_value": "Provide better mental health training and support for NHS staff"}, "numberOfSignatures": 221},
    {"abstract": {"_value": "The treason laws have not been updated for years. We believe that joining ISIS, or any other terrorist organisation which is an enemy of our country, should be classed as an act of treason. "}, "label": {"_value": "Update the treason laws to deal with British terrorists"}, "numberOfSignatures": 526},
    {"abstract": {"_value": "The Anglican Church insists that gay clergy with partners live a celibate life if they wish to continue working as priests or bishops. This is an unfair burden on gay clergy as well as being discriminatory within the 21st century workplace."}, "label": {"_value": "Make it illegal for the C of E to discriminate against gay non-celibate clergy"}, "numberOfSignatures": 1202},
    {"abstract": {"_value": "We, the undersigned, would like to request that a second referendum on the United Kingdom's membership of the EU should be held, due to the indication that demographics are now likely to have changed across the UK and many voters regret their decision on June 23rd."}, "label": {"_value": "Hold a second EU referendum on the grounds that demographics have now changed."}, "numberOfSignatures": 905}
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
