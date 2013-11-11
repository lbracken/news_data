# -*- coding: utf-8 -*-
"""
    news_data.scripts.cnn_file_count_writer
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Module that writes the file count results from
    'cnn_file_counter.sh' into mongoDB

    :license: MIT, see LICENSE for more details.
"""

from datetime import datetime

from db import mongo
import date_util


db_raw_articles = None


def init_db():
    global db_raw_articles

    # Init DB
    db_raw_articles = mongo.get_raw_articles()


def write_file_counts_to_db():

    # The following array is pasted in from the results of running 
    # 'cnn_file_count.sh'
    counts = [1932,1785,2042,1829,2009,1883,1873,2033,1887,2138,1914,1937,2190,1921,1178,1785,2301,2284,2536,2647,2503,2364,2109,2009,2266,1945,1995,1856,1973,1816,1971,1912,1728,1796,1768,1865,1889,1592,1923,2009,2240,2170,2218,2399,2020,1867,1689,1720,1449,1536,760,756,762,729,757,798,758,734,745,759,715,713,801,728,789,760,723,776,760,748,732,747,709,669,716,702,764,773,801,775,611,620,612,623,635,559,615,598,628,574,606,639,591,626,586,581,614,570,616,587,620,590,612,610,614,619,600,609,620,561,615,600,601,598,605,605,618,670,651,700,686,624,695,670,677,645,668,680,665,673,662,685,692,654,796,707,740,719,735,776,745,751,735,746,760,697,752,569,0,0,0,0,0,0,0,0]
	
    # The counts are per month, starting on Jan, 2000
    curr_month = datetime(2000, 01, 1)
    for count in counts:
        count_doc = {
            "count" : count,
            "published" : curr_month
        }
        db_raw_articles.insert(count_doc)
        curr_month = date_util.get_next_month(curr_month)

    print "... Done"


if __name__ == "__main__":
    print "--------------------------------------< cnn file count writer >----"
    print "  File Count Writer Started..."

    init_db()
    write_file_counts_to_db()