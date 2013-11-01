news_data
=========

  news_data is a project to process news articles and transcripts in order to provide visualizations and highlight trends.


###Supported News Sources##
  * CNN Transcripts (https://archive.org/details/cnn-transcripts-2000-2012)
  * ...


##Processing Pipeline

  The processing pipeline parses and analyzes a collection of articles before creating metrics which are persisted in mongoDB.  The pipeline consists of a number of python modules which are run independently, passing messages about completed work to the next module via RabbitMQ.

###PreReqs
  * Update news_data/settings.cfg with RabbitMQ configuration
  * Update news_data/settings.cfg with mongoDB configuration

###Pipeline Execution
  Start one or more instances of each pipeline module with the following commands.

    $ cd news_data/news_data
    $ python -m pipeline.file_scanner <DIR_WITH_ARTICLES>
    $ python -m pipeline.article_parser
    $ python -m pipeline.article_analyzer
    $ python -m pipeline.metric_writer
