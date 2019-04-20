import json
import time
import pandas as pd
import requests
from flask import jsonify
from threading import Thread
import schedule
import time
import os
import random

current_milli_time = lambda: int(round(time.time() * 1000))

def get_payload(data, header):    
    _payload = [
        {
            'headers': header,
            'body': data
        }
    ]
    return json.dumps(_payload)

def send(url, payload, header, time_out):
    try:
        requests.post(url, data=payload, headers=header, timeout=time_out)
    except Exception as e:
        pass

def post_review(data_frame=None, name='default'):

    print('>> Job Run {}'.format(name))
    URL_FLUME = 'http://localhost:9001'
    HEADER = {'content-type': 'text/plain'}
    TIME_OUT = 0.01
    SAMPLE = 2
    sample_frame = data_frame.sample(SAMPLE)

    for i in range(SAMPLE):
        body = str(current_milli_time())+'\t'+'\t'.join(sample_frame[i:i+1].values.flatten())
        payload = get_payload(body, HEADER)
        
        process = Thread(target=send, args=[URL_FLUME, payload, HEADER, TIME_OUT])
        process.start()       


if __name__ == '__main__':

    print('Sending data to flume agent source')

    _df_review = pd.read_csv("review_sample.csv")
    _df_review = _df_review[[
                             'stars',
                             'user_id',
                             'business_id'
                             ]]
    
    _df_review['stars'] = _df_review['stars'].apply(lambda x: str(x))

    # schedule.every(1).seconds.do(post_review, _df_review, name="every 1 second")
    schedule.every(1).to(5).seconds.do(post_review, _df_review, name="every 1 to 5 seconds")
    schedule.every(1).to(5).seconds.do(post_review, _df_review, name="every 1 to 5 seconds")
    schedule.every(1).to(5).seconds.do(post_review, _df_review, name="every 1 to 5 seconds")



    while 1:
        schedule.run_pending()
        # time.sleep(1)


