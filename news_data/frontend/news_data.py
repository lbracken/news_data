# -*- coding: utf-8 -*-
"""
    news_data.frontend.news_data
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Provides Flask based template rendering and web service support for
    news_data.  Its responsibilities for web service calls includes...
      * Handling incoming HTTP requests by parsing arguments
      * Call the appropriate service module for news data
      * Create a response from the data

    :license: MIT, see LICENSE for more details.
"""

from flask import Flask
from flask import render_template

app = Flask(__name__)


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
    return "db_stats"


if __name__ == '__main__':
    print "--------------------------------------< news_data web service >----"
    app.run(debug=True) # If running directly from the CLI, run in debug mode.