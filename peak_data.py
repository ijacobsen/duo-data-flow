import pandas as pd
import numpy as np
import time
  
# time taken to read data
s_time_chunk = time.time()
filename = 'learning_traces.csv'
chunk = pd.read_csv(filename, chunksize=1000)
e_time_chunk = time.time()
  
print("With chunks: ", (e_time_chunk-s_time_chunk), "sec")
df = pd.concat(chunk)
  
# data
print(df.head())
print(df.columns)

