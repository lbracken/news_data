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

import date_util
from service import stats_service
from service import metrics_service

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
    # TODO: Better error message for invalid requests...

    # Sample URL (Year 2000 Presidental Candidates)
    # http://localhost:5000/get_term_counts?terms=bush,gore,nader&time_start=946702800&time_end=978238800
    
    # Read and validate request arguments
    try:
        terms = get_terms_from_request(request)
        time_start = get_time_start_from_request(request)
        time_end = get_time_end_from_request(request)
        is_download = get_is_download_from_request(request)

        if verbose:
            print "-----------------------get_term_counts"
            print 'Terms:  %s' % terms
            print "Start:  %s" % time_start
            print "End:    %s" % time_end            
  
        # Ensure the request parameters are valid, otherwise return a 400
        if len(terms) == 0 or not is_valid_time_range(time_start, time_end):
            abort(400)

    except Exception,e :
        # If there are problems reading the request arguments, then
        # the request is bad.  Return a 400 HTTP Status Code - Bad
        # Request
        if verbose:
            print "  %s" % str(e)
            traceback.print_exc() 
        abort(400)

    # Get term counts for the response
    term_counts, granularity = metrics_service.get_term_counts(
            terms, time_start, time_end, verbose)
    
    #Construct the response body
    response_body = {
        "time_start"  : date_util.get_timestamp(time_start),
        "time_end"    : date_util.get_timestamp(time_end),
        "granularity" : granularity,        
        "terms"       : term_counts
    }

    # Create the response object and setup headers
    response = make_response(jsonify(response_body))

    if is_download:
        response.headers["Content-Disposition"] = \
                "attachment; filename=terms_data.json"

    return response


@app.route("/get_db_stats")
def get_db_stats():
    # TODO: Update queries to restrict to counts of daily and monthly METRIC data

    # Read and validate request arguments
    try:
        time_start = get_time_start_from_request(request)
        time_end = get_time_end_from_request(request)
        if verbose:
            print "-----------------------get_db_stats"
            print "Start:  %s" % time_start
            print "End:    %s" % time_end
  
        # Ensure the request parameters are valid, otherwise return a 400
        if not is_valid_time_range(time_start, time_end):
            abort(400)

    except Exception,e :
        # If there are problems reading the request arguments, then
        # the request is bad.  Return a 400 HTTP Status Code - Bad
        # Request
        if verbose:
            print "  %s" % str(e)
            traceback.print_exc() 
        abort(400)

    # Create the response by getting the counts of each collection,
    # then populate with the stats from each collection.
    response = stats_service.get_collection_counts(
            time_start, time_end, verbose)  
    response["raw_articles"] = stats_service.get_raw_articles_stats(
            time_start, time_end, verbose)
    response["parsed_articles"] = stats_service.get_parsed_articles_stats(
            time_start, time_end, verbose)
    response["analyzed_articles"] = stats_service.get_analyzed_articles_stats(
            time_start, time_end, verbose)
    
    # TODO: Provide metric data (daily & monthly) stats...

    return make_response(jsonify(response))


def get_time_start_from_request(request):
    """ Returns the start time provided in the request.

    """
    time_start_ms = request.args.get("time_start", 0)
    time_start = datetime.fromtimestamp(int(time_start_ms))
    return time_start


def get_time_end_from_request(request):
    """ Returns the end time provided in the request.

    """
    time_end_ms = request.args.get("time_end", 0)
    time_end = datetime.fromtimestamp(int(time_end_ms))
    return time_end


def get_terms_from_request(request):
    """ Returns the list of terms provided in the request.

    """
    terms = []
    raw_terms = request.args.get("terms", "").split(",")

    # Sanitize terms, and add non-empty elements to the list
    for term in raw_terms:
        term = term.strip().lower()
        if not term == "":
            terms.append(term)

    return terms


def get_is_download_from_request(request):
    """ Returns true if request is for a download, false otherwise.

    """
    is_download = request.args.get("download", "").strip().lower()
    return ("true" == is_download)


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