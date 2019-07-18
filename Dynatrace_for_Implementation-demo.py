#!/usr/bin/env python
# coding: utf-8

# # Preprocess data

import pandas as pd
import numpy as np
from IPython.display import display
import datetime
import pickle
from collections import deque
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from tqdm import tqdm_notebook as tqdm


data = pd.read_csv('../data/Final_data_df.csv', low_memory=False) 
pd.options.display.max_columns = None

#convert data type
data['@timestamp'] = pd.to_datetime(data['@timestamp'])
data['@timestamp'] = data['@timestamp'].astype('datetime64[s]')
data.dtypes
data.reset_index(inplace=True)
data.rename(columns={'@timestamp': 'timestamp', 'Message':'message', "index":'id'}, inplace=True)

#sort msg by timestamp from older to newer
data = data.sort_values("timestamp")
input_df = data

input_df.head(10)


# # Main Algorithm

# Input: 
# - input_stream: A stream of messages as list of namedtuples (index, timestamp, message)  
#   Can be a Double-ended queue (deque) or a list. deque might be more efficient
#   
# Parameters:
# - time_window: Time window to look back for deduplication (in hours)
# - min_sim_threshold: The minimum similarity threshold for decalring two messages as duplicates
# 
# Output: 
# - dedup_stream: A stream of deduplicated messages as namedtuples (index, timestap, message)
# - dup_msgs: A stream of duplicated messages as namedtuples (index, timestap, message, main_id, sim):   
#   main_id is the message index in the deduplicated stream. sim is the similarity between the duplicated message and the main message

# ### Select alerts within the time_window of new alert


def get_previous_alerts(new_alert, input_stream, time_window):
    '''
    Find all the messages within the time_window from the historic stream.    
    '''
    previous_alerts = []
    #find the earliest time in the range of previous time
    earliest_time = new_alert.timestamp - timedelta(hours=time_window)
    
    for old_msg in input_stream: # newer msgs are at the beginning of the stream deque
        if old_msg.timestamp >= earliest_time:
            if old_msg.id != new_alert.id:
                previous_alerts.append(old_msg)
        else: 
            break 
    return previous_alerts

#simulate message arrival

input_stream = deque([]) #a double-ended queue, newer msgs are added to the left of the queue
for i, msg in enumerate(input_df.itertuples()): #msg arrives
    input_stream.appendleft(msg)
    msgs_in_time_window = get_previous_alerts(msg, input_stream, time_window = 1)
    print("Number of msgs within the prior timewindow is {}".format(len(msgs_in_time_window)))
    if i >= 20:
          break


# ### Deduplicate new msg
# Compare levenshtein distance with the msgs in the prior timewindow
# - https://www.datacamp.com/community/tutorials/fuzzy-string-python


#http://jonathansoma.com/lede/algorithms-2017/classes/fuzziness-matplotlib/fuzzing-matching-in-pandas-with-fuzzywuzzy/

def check_dup(new_msg, prior_msg_stream, min_sim_threshold = 70, n_dup_msg = 1):
    '''
    Check if two strings have low enough editing distance to be considered duplicates 
    Args:
        Both msg and msg_stream are (collection of) namedtuples of (index, timestamp, message)
        min_sim_threshold: the similarity ratio for dedup (see https://github.com/seatgeek/fuzzywuzzy)
        n_dup_msg: a robustness parameter, if the number of duplicate messages are greater than this number then consider the
       new_msg to be a duplicate. Default = 1. 
    Returns:
        A tuple of dup (bool), main_id.  
        main_id is the message index in the deduplicated stream, None if the msg is not a duplicate
    '''
    dup = False
    main_id = None
    if len(prior_msg_stream) >= 1:
        choices = dict([(str(m.id), m.message) for m in prior_msg_stream])
        matches = process.extract(new_msg.message, choices, scorer = fuzz.ratio)
        #matches is a list of tuples of [(msg, sim_score), ...]
        matches = [m for m in matches if m[1] >= min_sim_threshold]
        # print(matches)
        if len(matches) >= n_dup_msg:
            dup = True
            main_id = str(matches[0][2])
    return dup, main_id


# ### Test the algorithm

#parameters
TIME_WINDOW = 1
MIN_SIM_THRESHOLD = 95

#for testing sequence
START_ID = 5000
TEST_N = 500


input_stream = deque([]) #a double-ended queue, newer msgs are added to the left of the queue
dedup_stream = deque([])
for i, msg in enumerate(input_df.itertuples()): #simulate msg arrives to the queue
    if i >= START_ID: #start test msg index
        input_stream.appendleft(msg)

        # ------ start dedup -----
        msgs_in_time_window = get_previous_alerts(msg, input_stream, time_window = TIME_WINDOW)
        # print("Number of msgs within the prior timewindow is {}".format(len(msgs_in_time_window)))
        dup, main_id = check_dup(msg, msgs_in_time_window, min_sim_threshold=MIN_SIM_THRESHOLD)
        if dup: 
            print('\x1b[3;37m {}, {}, {}, duplicate of id {} \x1b[0m'.format(msg.message, msg.timestamp, msg.id, main_id))
        else:
            print('\x1b[01;31m {} \x1b[0m'.format(msg.message), msg.timestamp, msg.id)
            dedup_stream.append(msg)
    if i >= START_ID + TEST_N: #end test msg index
          break
            
    #@todo: clear the input_stream every day

#parameters
TIME_WINDOW = 1
MIN_SIM_THRESHOLD = 95

input_stream = deque([]) #a double-ended queue, newer msgs are added to the left of the queue
dedup_stream = deque([])
for i, msg in enumerate(tqdm(input_df.itertuples(), total = input_df.shape[0])): #simulate msg arrives to the queue
    input_stream.appendleft(msg)

    # ------ start dedup -----
    msgs_in_time_window = get_previous_alerts(msg, input_stream, time_window = TIME_WINDOW)
    # print("Number of msgs within the prior timewindow is {}".format(len(msgs_in_time_window)))
    dup, main_id = check_dup(msg, msgs_in_time_window, min_sim_threshold=MIN_SIM_THRESHOLD)
    if not dup: 
        dedup_stream.append(msg)

            
    #@todo: clear the input_stream every day


dedup_stream_DF = pd.DataFrame(list(dedup_stream)).drop(["Index"], axis=1)
dedup_stream_DF.shape

dedup_stream_DF.to_csv('../output/dedup_stream_df.csv')


