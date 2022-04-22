import duo_lib
import os
import configparser

# create spark session
spark = duo_lib.create_spark_session(mode='local')
path = '../data_files/'

# read language reference table into df
filename = 'language-codes-full_json.json'
lang_df = duo_lib.read_lang_ref(spark, path, filename)

duo_lib.show_size('language reference', lang_df)
