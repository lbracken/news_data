# -*- coding: utf-8 -*-
"""
    news_data.news_data
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Provides Flask based template rendering and web service support for
    news_data.  Its responsibilities for web service calls includes...
      * Handling incoming HTTP requests by parsing arguments
      * Call the appropriate service module for news data
      * Create a response from the data

    :license: MIT, see LICENSE for more details.
"""

import argparse
from datetime import datetime
from datetime import timedelta
import traceback

from flask import abort
from flask import Flask
from flask import jsonify
from flask import make_response
from flask import render_template
from flask import request

from service import stats_service

app = Flask(__name__)
verbose = False
min_time_start = datetime.fromtimestamp(946702800)


@app.route("/")
def main_page():
    return render_template("index.html")


@app.route("/db_stats")
def db_stats_page():
    return render_template("db_stats.html")


@app.route("/get_term_counts")
def get_term_counts():
    return "term_counts"


@app.route("/get_db_stats")
def get_db_stats():
    # TODO: Require password to access data...
    # TODO: Update queries to restrict to counts of daily and monthly METRIC data

    # Read and validate request arguments
    try:
        time_start = get_time_start_from_request(request)
        time_end = get_time_end_from_request(request)
        if verbose:
            print "-----------------------getDBStats"
            print "Start:  %s" % time_start
            print "End:    %s" % time_end
  
        # Ensure the request parameters are valid, otherwise return a 400
        if not is_valid_time_range(time_start, time_end):
            abort(400)

    except Exception,e :
        # If there are problems reading the request arguments, then
        # the request is bad.  Return a 400 HTTP Status Code - Bad
        # Request
        #if verbose:
        #    print "  %s" % str(e)
        #    traceback.print_exc() 
        abort(400)


    response = {}
    response["raw_articles"] = \
            stats_service.get_raw_articles_stats(time_start, time_end)
    response["parsed_articles"] = \
            stats_service.get_parsed_articles_stats(time_start, time_end)            
    response["analyzed_articles"] = \
            stats_service.get_analyzed_articles_stats(time_start, time_end)

    # Create the response object and setup headers
    resp = make_response(jsonify(response))
    return resp


def get_time_start_from_request(request):
    """ Returns the start time provided in the request.

    """
    time_start_ms = request.args.get("time_start", 0)
    print ">>> %s" % time_start_ms
    time_start = datetime.fromtimestamp(int(time_start_ms))
    return time_start


def get_time_end_from_request(request):
    """ Returns the end time provided in the request.

    """
    time_end_ms = request.args.get("time_end", 0)
    time_end = datetime.fromtimestamp(int(time_end_ms))
    return time_end


def is_valid_time_range(time_start, time_end):
    """ Returns True if the given request parameters are valid,
        False otherwise

    """
    # Ensure Start is before End
    if (time_start > time_end):
        print "Invalid Request - Start time is after end time."
        return False

    # Start and End are within reasonable bounds
    if (time_start < min_time_start):
        print "Invalid Request - Start time is too early"
        return False
  
    return True 
  

def parse_args():
    """ Parse the command line arguments

    """
    global verbose

    parser = argparse.ArgumentParser(description="news_data web service")
    parser.add_argument("-v", "--verbose", action="store_true",
            help="Make the operation talkative")
    args = parser.parse_args()   
    
    verbose = args.verbose
    return args


if __name__ == "__main__":
    args  = parse_args()

    print "--------------------------------------< news_data web service >----"
    app.run(debug=True) # If running directly from the CLI, run in debug mode.
