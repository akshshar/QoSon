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
import threading
import sys

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

ABS_PATH = os.path.dirname(os.path.abspath(__file__))
httplib.HTTPConnection.debuglevel = 1

LAN_URL="http://192.168.122.1"

QOSON_URL=LAN_URL+":6060"
MARATHON_URL=LAN_URL+":8080"
INTERVAL = 10

appNwObjDict = {} 

class appNwContext:
    def __init__(self, param_dict):
        self.param_dict= param_dict 

def launchQosonTasks(qosonData):
    #Start the network monitoring tasks
    response = {"result" : "error", "msg" : ""}
    try:
        print "\n\n\n Launching Network monitoring Tasks \n\n"
        resp =  requests.post(QOSON_URL+"/appq",
                              data=json.dumps(qosonData),
                              headers={"Content-Type": "application/json"})

        response = {"result" : "success", "msg" : resp.content}
    except Exception as e:
        print "Unable to create an app in Marathon"
        print e
    return json.dumps(response)
 
def glean():
    while True:                                                                                                                                                                     
        try:
            resp = requests.get(MARATHON_URL+"/v2/apps/") 
            #pdb.set_trace()
            for app in json.loads(resp.content)["apps"]:
                #pdb.set_trace()
                if app["id"] in appNwObjDict:
                    qosonData = appNwObjDict[app["id"]].param_dict
                    qosonData["app_id"] = app["id"]
                    launchQosonTasks(qosonData)
                    time.sleep(1)
        except Exception as e:
            print "Unable to get a list of running apps from Marathon"
            print e
        time.sleep(INTERVAL)                    
    
def pretty_print_POST(req):
    """
    At this point it is completely built and ready
    to be fired; it is "prepared".

    However pay attention at the formatting used in 
    this function because it is programmed to be pretty 
    printed and may differ from the actual request.
    """
    print('{}\n{}\n{}\n\n{}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        req.body,
    ))



def register():
    #Construct the json data for registration
    try:
        registerData = {}
        registerData['name'] = "marathon-qoson-client"
        registerData['url'] = LAN_URL+":9090/constraint"
        registerData['method'] = "GET"
        registerData['data'] = ""
        registerData['header'] = {"Content-Type": "application/json"}
        registerData["network_role"] = "tor"

        requests.post(QOSON_URL+"/register", headers=registerData['header'], data=json.dumps(registerData))
    except Exception as e:
        print "Unable to Register with QoSon"
        print e


app = Flask(__name__)

@app.route('/constraint', methods=['GET'])
def constraint():
    response = {"result" : "error", "msg" : ""}
    try:

        marathon_appId = request.args.get('app_id')

        update_data = {"constraints": [["rack", "UNLIKE", request.args.get('rack')], ["device-type", "LIKE", "server"]]}
        resp = requests.put(MARATHON_URL+"/v2/apps/"+marathon_appId,
          		      data=json.dumps(update_data),
                              headers={"Content-Type": "application/json"},
                              params={"force":"true"})

        response = {"result" : "success", "msg" : resp.text}
    except Exception as e:
        print "Unable to apply constraint"
        print e

    return jsonify(response)

@app.route('/marathon_create_app', methods=['POST'])
def marathon_create_app():
    global appNwObjDict 
    response = {"result" : "error", "msg" : ""}
    try:
        appData = json.loads(request.data)
        qosonData = appData["networkResources"]
        qosonData["app_id"] = "/"+appData["id"]

        #Store the app_network context for subsequent runs
        appNwObjDict["/"+appData["id"]] = appNwContext(appData["networkResources"])
        del appData["networkResources"]
 
        print json.dumps(appData)
        resp = requests.post(MARATHON_URL+"/v2/apps/",
                              data=json.dumps(appData),
                              headers={"Content-Type": "application/json"})

        launchQosonTasks(qosonData) 
        response = {"result" : "success", "msg" : resp.text}
    except Exception as e:
        print "Unable to create an app in Marathon"
        print e

    return jsonify(response)

    
if __name__=='__main__':
    register()
    thread = threading.Thread(target=glean, args=())
    thread.daemon = True                            # Daemonize thread                                                                                                              
    thread.start()                                  # Start the execution          
    app.run(host='0.0.0.0',port=9090, debug=True, use_reloader=False)
 
