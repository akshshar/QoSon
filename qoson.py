#!/usr/bin/env python

# author: akshshar 
# https://github.com/akshshar/qoson

from multiprocessing import Process, Queue
import os, time, subprocess, json, shlex
import os.path as path
from subprocess import Popen, PIPE
from flask import Flask, render_template, request, session, redirect, url_for, jsonify
import requests
from requests.auth import HTTPDigestAuth
import urlparse, logging, httplib
from threading import Timer
import hashlib
import pdb

from argparse import ArgumentParser
from service import NetmonScheduler, appData, registerData
import sys

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

ABS_PATH = os.path.dirname(os.path.abspath(__file__))
httplib.HTTPConnection.debuglevel = 1


######################################################################
## globals

APP_NAME = "QoSon"


######################################################################
## command line arguments


# SharedQueue to pass received app data to the scheduler
sharedAppQueue = None 

#SharedQueue to pass registered clients to the scheduler
sharedClientQueue = None

app = Flask(__name__)

def startRestServer():
    app.run(host='0.0.0.0',port=6060, debug=True, use_reloader=False)

@app.route('/register', methods=['POST'])
def register():
    response = {"result" : "error", "msg" : ""}
    try: 
        registerJson = json.loads(request.data)
        registerDataObj = registerData(registerJson["name"],
					registerJson["url"],
				    	registerJson["method"],
					registerJson["data"],
					registerJson["header"],
                                        registerJson["network_role"]) 

        global sharedClientQueue
        #Pushing to the Netmon Scheduler appQueue using the sharedAppQueue
        sharedClientQueue.put(registerDataObj)
        response = {"result" : "success", "msg" : registerJson}  
    except Exception as e:
        print "Failed to parse registration data"                                                                                                                                        
        print e          
    return jsonify(response)

@app.route('/appq', methods=['POST'])
def appq():
    response = {"result" : "error", "msg" : ""}
    try:
        appJson = json.loads(request.data)

        appDataParams = ["app_id", "tool"]
        appDatadict = {}

        for param in appDataParams:
            appDatadict[param] = appJson[param]
            del appJson[param]
 
    
        #Create the network resources dictionary

        appDataObj = appData(appDatadict["app_id"],
                             appDatadict["tool"], 
                             appJson) 
        global sharedAppQueue
        #Pushing to the Netmon Scheduler appQueue using the sharedAppQueue
        sharedAppQueue.put(appDataObj)
        response = {"result" : "success", "msg" : appDatadict}
    except Exception as e:
        print "Failed to parse application data"
        print e
    
    return jsonify(response)

def parse_cli_args ():
    parser = ArgumentParser(prog="QoSon", add_help=True,
                            description="QoSon, a distributed framework for application driven network monitoring and SLA enablement, based on Apache Mesos")

    parser.add_argument("-e", "--exe_path", nargs=1,
                        help="Path to the executor")

    parser.add_argument("-m", "--master", nargs=1,
                        help="location for one of the masters")

    parser.add_argument("--cpu", nargs=1, type=float, default=[1],
                        help="CPU allocation per task, as CPU count")
    parser.add_argument("--mem", nargs=1, type=float, default=[1],
                        help="MEM allocation per task, as MB/shard")

    parser.add_argument("--log", nargs=1, default=["DEBUG"],
                        help="logging level: INFO, DEBUG, WARNING, ERROR, CRITICAL")

    return parser.parse_args()


if __name__=='__main__':
    sharedAppQueue = Queue()
    sharedClientQueue = Queue() 

    # interpret CLI arguments
    args = parse_cli_args()

    websrvr = Process(target=startRestServer, args=())
    websrvr.start()
    # set up logging
    numeric_log_level = getattr(logging, args.log[0], None)

    if not isinstance(numeric_log_level, int):
        raise ValueError("Invalid log level: %s" % loglevel)

    logging.basicConfig(format="%(asctime)s\t%(levelname)s\t%(message)s", 
                        filename="qoson.log", 
                        filemode="w",
                        level=numeric_log_level
                        )
    logging.debug(args)

    logging.info("%s: running The QoSon Framework atop an Apache Mesos cluster", APP_NAME)
    logging.info(" ...with master %s", args.master[0])

    try:
        master_uri = args.master[0]
        exe_path = os.path.abspath(args.exe_path[0])

        logging.debug("Starting the scheduler.....")
        # run Mesos driver to launch Framework and manage resource offers
        driver,sched = NetmonScheduler.start_framework(master_uri, exe_path, args.cpu[0], args.mem[0], 0)
       
 
        sched.appQueue = sharedAppQueue
        sched.clientQueue = sharedClientQueue

        logging.debug("Stopping the scheduler.....")
        NetmonScheduler.stop_framework(driver)
    except ImportError as e:
        logging.critical("Python module 'mesos' has not been installed", exc_info=True)
        raise


