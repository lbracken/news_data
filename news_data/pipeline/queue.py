# -*- coding: utf-8 -*-
"""
    news_data.pipeline.queue
    ~~~~~~~~~~~~~~~~~~~~~~~~ 

    This module supports interaction with RabbitMQ.

    Usage: Provides three functions for each exhange/queue pair.

      * init_<queue> : Declares the queue, exchange and binding
      * publish_<queue> : Publish a msg to the given queue/exchange
      * consume_<queue> : Start consuming msgs from the given queue
    
    :license: MIT, see LICENSE for more details.
"""

import ConfigParser
import logging

import pika

# These values set from config file
queue_user = None
queue_pass = None
queue_host = None
queue_port = None
queue_vhost = None

# Define Exchange, Routing Key and Queue names
scanned_files_ex    = 'scanned_files_ex'
scanned_files_rtkey = 'scanned_files_rtkey'
scanned_files_queue = '01_scanned_files'

parsed_files_ex    = 'parsed_files_ex'
parsed_files_rtkey = 'parsed_files_rtkey'
parsed_files_queue = '02_parsed_files'

analyzed_articles_ex    = 'analyzed_articles_ex'
analyzed_articles_rtkey = 'analyzed_articles_rtkey'
analyzed_articles_queue = '03_analyzed_articles' 


def init_config():
    """ Read RabbitMQ connection settings from config file

    """
    global queue_user, queue_pass, queue_host, queue_port, queue_vhost

    config = ConfigParser.SafeConfigParser()
    config.read("../settings.cfg")

    queue_user = config.get("rabbitmq", "queue_user")
    queue_pass = config.get("rabbitmq", "queue_pass")
    queue_host = config.get("rabbitmq", "queue_host")
    queue_port = config.getint("rabbitmq", "queue_port")
    queue_vhost = config.get("rabbitmq", "queue_vhost")


def init_connection():
    print "  RabbitMQ Conn: %s@%s:%s" % (queue_user, queue_host, queue_port)
    print "  Pika Version: %s" % pika.__version__

    logging.basicConfig(level=logging.WARN)
    queue_conn_str = 'amqp://%s:%s@%s:%d/%s' % (queue_user, queue_pass,
            queue_host, queue_port, queue_vhost)

    queue_params = pika.URLParameters(queue_conn_str)
    connection = pika.BlockingConnection(queue_params)
    channel = connection.channel()
    return connection, channel


def close_connection(connection):
    connection.close()


def init_queue(channel, exchange_name, routing_key_name, queue_name, 
        init_exchange_and_binding):
    """ Declares a queue, and optionally declares the exchange
        and binding

    """
    channel.queue_declare(queue=queue_name)
    if init_exchange_and_binding:

        channel.exchange_declare(
            exchange=exchange_name,
            type='direct',
            passive=False,
            durable=True,
            auto_delete=False)

        channel.queue_bind(
            queue=queue_name,
            exchange=exchange_name,
            routing_key=routing_key_name)


def publish(channel, exchange_name, routing_key_name, msg_body):
    channel.basic_publish(
        exchange=exchange_name,
        routing_key=routing_key_name,
        body=msg_body)

    return True


def consume(channel, callback, queue_name):
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue=queue_name)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print ">>> Keyboard Interrupt -- Stopping queue consumption"
        channel.stop_consuming()




def init_scanned_files(channel, init_exchange_and_binding=False):
    init_queue(channel,
        scanned_files_ex,
        scanned_files_rtkey,
        scanned_files_queue,
        init_exchange_and_binding)


def publish_scanned_file(channel, msg_body):
    return publish(channel, scanned_files_ex, scanned_files_rtkey, msg_body)


def consume_scanned_files(channel, callback):
    consume(channel, callback, scanned_files_queue)




def init_parsed_files(channel, init_exchange_and_binding=False):
    init_queue(channel,
        parsed_files_ex,
        parsed_files_rtkey,
        parsed_files_queue,
        init_exchange_and_binding)  


def publish_parsed_file(channel, msg_body):
    return publish(channel, parsed_files_ex, parsed_files_rtkey, msg_body)


def consume_parsed_files(channel, callback):
    consume(channel, callback, parsed_files_queue)




def init_analyzed_articles(channel, init_exchange_and_binding=False):
    init_queue(channel,
        analyzed_articles_ex,
        analyzed_articles_rtkey,
        analyzed_articles_queue,
        init_exchange_and_binding)  


def publish_analyzed_article(channel, msg_body):
    return publish(channel, analyzed_articles_ex, analyzed_articles_rtkey,
                   msg_body)


def consume_analyzed_articles(channel, callback):
    consume(channel, callback, analyzed_articles_queue)


# Initialize config when loading module
init_config()