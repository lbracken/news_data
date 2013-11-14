news_data
=========

news_data is a project to process news articles and transcripts in order to provide visualizations and highlight trends. The project is divided into two parts...
  1. The processing pipeline to parse and analyze articles and transcripts. Results are stored in mongoDB.
  2. A Flask based application that runs the WebUI and web services which provide access to the data stored in mongoDB.


###Supported News Sources##
  * CNN Transcripts (https://archive.org/details/cnn-transcripts-2000-2012)
  * ...


##Processing Pipeline

The processing pipeline parses and analyzes a given collection of articles and transcripts.  The resulting analysis is stored in mongoDB as metric data at a daily and monthly level of granularity.  The pipeline itself consists of a number of python modules which are run independently, passing messages about completed work to the next module in the pipeline via RabbitMQ.

###PreReqs
  * Update news_data/settings.cfg with RabbitMQ configuration
  * Update news_data/settings.cfg with mongoDB configuration

###Pipeline Execution
  Start one or more instances of each pipeline module with the following commands.

    $ python -m pipeline.file_scanner <DIR_WITH_ARTICLES>
    $ python -m pipeline.article_parser
    $ python -m pipeline.article_analyzer
    $ python -m pipeline.metric_writer
    
  The modules can be run with -v for additional details or -u for more frequent updates while processing.

##WebUI + web services

To run the Flask based web application:

    $ python -m news_data

The WebUI (and web services) are then available at http://localhost:5000.

In order for the WebUI to show something interesting, it must first have metrics in mongoDB.  This can be done by executing the processing pipeline, or by simply downloading and loading the already processed results (TODO: link to download of results).
