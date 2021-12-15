import pandas as pd
import numpy as np
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
from pyspark.sql.functions import date_format, isnan, isnull, when, count, col
import configparser
import os
import json
import boto3
import codecs
import re
from operator import and_
from functools import reduce
from datetime import datetime

# create spark session
def create_spark_session():
    '''
    creates a spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory','4G') \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .getOrCreate()
    return spark

# ==============================================================================
# ==============================================================================
#                        __                        __   
#         ____ ___  ____/  |_____________    _____/  |_ 
#       _/ __ \\  \/  /\   __\_  __ \__  \ _/ ___\   __\
#       \  ___/ >    <  |  |  |  | \// __ \\  \___|  |  
#        \___  >__/\_ \ |__|  |__|  (____  /\___  >__|  
#            \/      \/                  \/     \/     
# ==============================================================================
# ==============================================================================

# read learning traces
def read_learning_traces(spark, s3_path, filename):
    df = spark.read.option("header","true").csv(os.path.join(s3_path, filename))
    df = df.limit(2000000) # data too big for jupyter
    return df

# read language reference table
def read_lang_ref(spark, s3_path, filename):
    df = spark.read.json(os.path.join(s3_path, filename))
    return df

# read lexeme table
def read_lex_ref(spark, s3_bucket, filename):

    s3 = boto3.resource("s3")
    s3_object = s3.Object(s3_bucket, filename)
    line_stream = codecs.getreader("utf-8")

    # make data ingestible... pyspark not able to infer schema as-is
    lex_list = []
    for line in line_stream(s3_object.get()['Body']):
        all_split = line.split()
        lex_list.append([all_split[0], all_split[1], ' '.join(all_split[2:])])

    # load into dataframe with a defined schema
    from pyspark.sql.types import ArrayType, StructField, StructType, StringType
    schema = StructType([
        StructField('code', StringType(), True),
        StructField('type', StringType(), True),
        StructField('description', StringType(), True)])
    lex_df = spark.createDataFrame(lex_list, schema)

    # filter word types... we only want parts of speech in dimension table
    lex_df = lex_df.filter(lex_df.type == 'POS').select(['code', 'description'])
    lex_df = lex_df.withColumnRenamed('description', 'part_of_speech')
    
    return lex_df

# show size of datasets
def show_size(name, df):
    print('{} dataset has {} rows'.format(name, df.count()))

# ==============================================================================
# ==============================================================================
#     __                                 _____                     
#   _/  |_____________    ____   _______/ ____\___________  _____  
#   \   __\_  __ \__  \  /    \ /  ___/\   __\/  _ \_  __ \/     \ 
#   |  |  |  | \// __ \|   |  \\___ \  |  | (  <_> )  | \/  Y Y  \
#   |__|  |__|  (____  /___|  /____  > |__|  \____/|__|  |__|_|  /
#                    \/     \/     \/                          \/ 
# ==============================================================================
# ==============================================================================

# remove any duplicates from each dataframe
def check_duplicate_data(df, name):
    clean_df = df.distinct()
    
    # if interested in the details of data check then uncomment...
    # otherwise for sake of runtime, just find distinct values and return
    '''
    clean_count = clean_df.count()
    df_count = df.count()
    if clean_count != df_count:
        msg = ('{} duplicates dropped from {}'.format(df_count - clean_count, name))
    else:
        msg = 'data good'
    return (clean_df, msg)
    '''
    return (clean_df, 'data good')

# drop rows with missing data from each dataframe
def check_missing_data(df, name, cols):
    
    # if interested in the details of data check then uncomment...
    # otherwise for sake of runtime, just drop missing values
    '''
    # count number of missing data for each column
    counts = df.select([count(when(isnan(c) | isnull(c), c)).alias(c) for c in cols]).collect()[0][:]

    # check if any row contains NaN or None
    if any(counts) > 0:
        msg = 'missing values detected from {}... cleaning up'.format(name)

        # drop rows with missing data
        df = df.where(reduce(and_, (~isnan(df[c]) & ~isnull(df[c]) for c in cols)))
    else:
        msg = 'data good'
    return (df, msg)
    '''
    df = df.where(reduce(and_, (~isnan(df[c]) & ~isnull(df[c]) for c in cols)))
    return (df, 'data good')

# wrapped up data checks
def check_data(df, name, cols=None):
    
    # check for duplicates
    (df, msg) = check_duplicate_data(df, name)
    if msg != 'data good':
        print(msg)
    
    # default config --> check for missing in all columns
    if cols == None:
        cols = df.columns

    # check for missing data
    (df, msg) = check_missing_data(df, name, cols)
    if msg != 'data good':
        print(msg)
    
    # return clean df
    return df

def create_users_table(df):
    
    # create users table... count unique timestamps to find number of sessions
    users = (df.select(['user_id', 'timestamp']).drop_duplicates()).groupBy('user_id').count()
    dim_users = users.withColumnRenamed('count', 'number_of_sessions')
    return df

def create_times_table(df):
    
    # create times table
    times = df.select('timestamp')
    times = times.withColumnRenamed('timestamp', 'epoch')

    # convert epochs to timestamps
    timestamp = udf(lambda x: str(datetime.fromtimestamp(int(x))))
    times = times.withColumn('timestamp', timestamp(times.epoch))

    # extract granular times data, store as new columns
    get_hour = udf(lambda x: datetime.fromtimestamp(int(x)).hour)
    get_day = udf(lambda x: datetime.fromtimestamp(int(x)).day)
    get_week = udf(lambda x: datetime.fromtimestamp(int(x)).isocalendar()[1])
    get_month = udf(lambda x: datetime.fromtimestamp(int(x)).month)
    get_year = udf(lambda x: datetime.fromtimestamp(int(x)).year)
    get_weekday = udf(lambda x: datetime.fromtimestamp(int(x)).weekday())
    times = times.withColumn('hour', get_hour(times.epoch))
    times = times.withColumn('day', get_day(times.epoch))
    times = times.withColumn('week', get_week(times.epoch))
    times = times.withColumn('month', get_month(times.epoch))
    times = times.withColumn('year', get_year(times.epoch))
    dim_times = times.withColumn('weekday', get_weekday(times.epoch))
    
    return dim_times

def create_langs_table(spark, df, lang_df):
    
    # find languages that are used
    learn_langs = df.select(['learning_language']).drop_duplicates()
    ui_langs = df.select(['ui_language']).drop_duplicates()

    # concat and drop duplicates
    used_list = list(learn_langs.toPandas()['learning_language']) + list(ui_langs.toPandas()['ui_language'])
    used_list = list(set(used_list))

    # create table via Pandas
    dim_langs = spark.createDataFrame(pd.DataFrame({'alpha2_code': used_list}))
    
    # join with reference table
    dim_langs = dim_langs.join(lang_df, dim_langs.alpha2_code == lang_df.alpha2).drop('French', 'alpha2', 'alpha3-b', 'alpha3-t')
    
    return dim_langs


def create_words_table(df, lex_df):
    
    # create df
    words = df.select(['lexeme_id', 'lexeme_string', 'learning_language'])
    words = words.withColumnRenamed('learning_language', 'language')
    words = words.drop_duplicates()

    # extract granular data, create new cols
    get_lemma = udf(lambda x: re.search('/(.*?)<', x)[0][1:-1])
    get_surface = udf(lambda x: re.search('(.*?)/', x)[0][0:-1])
    get_pos = udf(lambda x: re.search('<(.*?)>', x)[0][1:-1])
    words = words.withColumn('lemma', get_lemma(words.lexeme_string))
    words = words.withColumn('surface', get_surface(words.lexeme_string))
    words = words.withColumn('pos', get_pos(words.lexeme_string))
    words = words.drop('lexeme_string')

    # look-up part of speech, save to table
    dim_words = words.join(lex_df, words.pos == lex_df.code, 'left').drop('code', 'pos')

    return dim_words


def create_wordviews_table(df):
    
    # create word views dataframe
    wv_df = df.select(['timestamp', 'user_id', 'learning_language', 'ui_language',
                       'lexeme_id', 'delta', 'history_seen', 'history_correct', 
                       'session_seen', 'session_correct'])

    # calculate statistics
    percent_correct = udf(lambda x, y: 100 * round(float(x)/float(y), 2))

    wv_df = wv_df.withColumn('session_pct', percent_correct(wv_df.session_correct, wv_df.session_seen))
    wv_df = wv_df.withColumn('history_pct', percent_correct(wv_df.history_correct, wv_df.history_seen))

    # drop granular
    wv_df = wv_df.drop('history_seen', 'history_correct', 'session_seen', 'session_correct')

    # epoch to timestamp... could have JOIN'd with times table... but better to transform existing column than big join
    timestamp = udf(lambda x: str(datetime.fromtimestamp(int(x))))
    fact_wordviews = wv_df.withColumn('timestamp', timestamp(wv_df.timestamp))
    
    return fact_wordviews

# check uniqueness of primary key for dimension tables
def qc_check_pk_unique(df, pkey):
    table_size = df.count()
    pk_count = df.select([pkey]).count()
    
    if table_size != pk_count:
        return False
    else:
        return True

# count to ensure completeness of data
def qc_source_count(s_df, dim_df, dimension='*'):
    
    source_count = s_df.select([dimension]).drop_duplicates().count()
    dim_count = dim_df.count()
    
    if source_count != dim_count:
        return False
    else:
        return True

# ==============================================================================
# ==============================================================================
#                    .__                    .___
#                    |  |   _________     __| _/
#                    |  |  /  _ \__  \   / __ | 
#                    |  |_(  <_> ) __ \_/ /_/ | 
#                    |____/\____(____  /\____ | 
#                                    \/      \/ 
# ==============================================================================
# ==============================================================================

def upload_parquet(s3_path, output_dir, df, name):
    df.write.mode('overwrite').parquet(s3_path + output_dir + name)

# ==============================================================================
# ==============================================================================
#                 ________ __   ___________ ___.__.
#                / ____/  |  \_/ __ \_  __ <   |  |
#               < <_|  |  |  /\  ___/|  | \/\___  |
#                \__   |____/  \___  >__|   / ____|
#                   |__|           \/       \/    
# ==============================================================================
# ==============================================================================

def languages_available(fact, dim):
    # form language pairs table
    pairs = fact.select(['learning_language', 'ui_language']).drop_duplicates()

    # match language pairs alpha2 codes with English language names
    langs = pairs.join(dim, pairs.learning_language == dim.alpha2_code, 'inner').drop('alpha2_code')
    langs = langs.withColumnRenamed('English', 'Learning Language')
    langs = langs.join(dim, pairs.ui_language == dim.alpha2_code, 'inner').drop('alpha2_code')
    langs = langs.withColumnRenamed('English', 'UI Language')

    return langs

def num_views_pair(df):
    # count how many word views for each pair
    df = df.select(['learning_language', 'ui_language']).groupBy(['learning_language', 'ui_language']).count()
    df = df.withColumnRenamed('count', 'number of words seen for pair')
    return df

def num_users_pair(df):
    # count how many users for each language pair
    df = df.select(['learning_language',
                    'ui_language',
                    'user_id']).distinct().groupBy(['learning_language',
                                                    'ui_language']).count()
    df = df.withColumnRenamed('count', 'number of learners for pair')
    return df