import json
import time
import pandas as pd
import requests
from flask import jsonify
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

def post_review(data_frame=None, name='default'):

    print('>> Job Run {}'.format(name))
    URL_FLUME = 'http://localhost:9001'
    HEADER = {'content-type': 'text/plain'}
    TIME_OUT = 5
    SAMPLE = 1
    sample_frame = data_frame.sample(SAMPLE)

    body = str(current_milli_time())+'\t'+'\t'.join(sample_frame[0:1].values.flatten())
    payload = get_payload(body, HEADER)
    #print(payload)
    response = requests.post(URL_FLUME, data=payload, headers=HEADER, timeout=TIME_OUT)
    #print(response)
    #print(response.text)


if __name__ == '__main__':

    print('Sending data to flume agent source')

    _df_review = pd.read_csv("review_sample.csv")
    _df_review = _df_review[[
                             'stars',
                             'user_id',
                             'business_id'
                             ]]
    
    _df_review['stars'] = _df_review['stars'].apply(lambda x: str(x))
    # _df_review = _df_review[['review_id',
    #                          'business_id',
    #                          'user_id',
    #                          'cool',
    #                          'funny',
    #                          'stars',
    #                          'useful',
    #                          'text']]

    schedule.every(1).seconds.do(post_review, _df_review, name="every 1 second")
    schedule.every(1).seconds.do(post_review, _df_review, name="every 1 second")
    schedule.every(1).seconds.do(post_review, _df_review, name="every 1 second")
    schedule.every(2).seconds.do(post_review, _df_review, name="every 2 second")
    schedule.every(3).seconds.do(post_review, _df_review, name="every 3 second")
    schedule.every(1).to(5).seconds.do(post_review, _df_review, name="every 1 to 5 seconds")
    schedule.every(1).to(5).seconds.do(post_review, _df_review, name="every 1 to 5 seconds")


    while 1:
        schedule.run_pending()
        # time.sleep(1)


