# -*- coding: utf-8 -*-
"""
    news_data.pipeline.file_scanner
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    [Pipeline Step 1 of 4]   

    This module recursively scans a file system directory for news
    articles, publishing each filename it finds to the 'scanned_files'
    queue.

    :license: MIT, see LICENSE for more details.
"""

import argparse
import os
import re
import sys

import queue


# Constants
scan_match_pattern = ".*html$"

verbose = False
updt_freq = 1000


def add_scanned_file_to_queue(channel, filename):
    """ Publish the given filename to the queue

    """
    if verbose:
        print "    Adding to Queue: %s" % filename

    return queue.publish_scanned_file(channel, filename)
    return True


def scan_directory(directory):
    """ Recursively scan the given directory file new articles

    """    
    print "  Starting scan..."
    print "  Directory: %s" % directory

    files_queued = 0

    # Initialize a connection and channel to RabbitMQ
    connection, channel = queue.init_connection()
    queue.init_scanned_files(channel, True)
    for root, dirs, files in os.walk(directory):

        # Walk dirs and files in alphabetical order
        dirs.sort()
        files.sort()

        for file in files:
            filename = os.path.join(root, file)

            # Only queue files that match the pattern
            if re.match(scan_match_pattern, filename):
                add_scanned_file_to_queue(channel, filename)
                files_queued += 1

                # Update status...
                if files_queued % updt_freq == 0:
                    print "    * Files Queued: %d..." % files_queued            

        # Ignore any svn dirs
        if '.svn' in dirs:
            dirs.remove('.svn')

    queue.close_connection(connection)

    print ""
    print "  ... scan complete"
    print "    Files Queued: %d" % files_queued
    print ""


def parse_args():
    """ Parse the command line arguments

    """
    global verbose, updt_freq

    parser = argparse.ArgumentParser()
    parser.add_argument("directory", help="Directory to scan for files")
    parser.add_argument("-v", "--verbose", action='store_true',
            help="Make the operation talkative")
    parser.add_argument("-u", "--updt_freq", type=int, default=1000,
            help="Frequency to print an update")    
    args = parser.parse_args()   
    
    verbose = args.verbose
    updt_freq = args.updt_freq
    return args


if __name__ == "__main__":
    args  = parse_args()  

    print "-----------------------------------------------< file_scanner >----"
    scan_directory(args.directory)