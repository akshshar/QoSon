import os
from flask import Flask, render_template, request, session, redirect, url_for, jsonify
import requests
from requests.auth import HTTPDigestAuth
import logging
import httplib
httplib.HTTPConnection.debuglevel = 1
import subprocess
import shlex
import json
from subprocess import Popen, PIPE
import os.path as path
from threading import Timer
import hashlib
import pdb
import re

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

ABS_PATH = os.path.dirname(os.path.abspath(__file__))

QOSON_URL="http://192.168.122.29:6060"
LAN_URL="http://192.168.122.1"
HOST="192.168.122.201"
PORT="8181"

GET_PCE_URL = "http://"+str(HOST)+":"+str(PORT)+"/restconf/operational/network-topology:network-topology/topology/pcep-topology"
CREATE_PCE_PE1_P1_P2_PE2_URL = "http://"+str(HOST)+":"+str(PORT)+"/restconf/operations/network-topology-pcep:add-lsp" 
UPDATE_PCE_PE1_PE2_URL = "http://"+str(HOST)+":"+str(PORT)+"/restconf/operations/network-topology-pcep:update-lsp"
DELETE_PCE_PE1_PE2_URL = "http://"+str(HOST)+":"+str(PORT)+"/restconf/operations/network-topology-pcep:remove-lsp" 

HEADERS = {"Content-Type" : "application/xml", "Accept" : "application/xml"}

def register():
    #Construct the json data for registration
    try:
        registerData = {}
        registerData['name'] = "mesos_odl_app"
        registerData['url'] = LAN_URL+":6302/switchover"
        registerData['method'] = "GET"
        registerData['data'] = ""
        registerData['header'] = {"Content-Type": "application/json"}
        registerData["network_role"] = "gw"

        requests.post(QOSON_URL+"/register", headers=registerData['header'], data=json.dumps(registerData))
    except Exception as e:
        print "Unable to Register with QoSon"
        print e


app = Flask(__name__)

@app.route('/update')
def update():
    response = {"result" : "error", "msg" : "NA"} 

    with open(ABS_PATH+"/update_pe1_pe2_pce_xml", "r") as file:
        data = file.read()
    try:
      resp = requests.post(UPDATE_PCE_PE1_PE2_URL, data=data, headers=HEADERS)
      if resp.status_code == 200:
          print "Path update successful!"
          response = {"result" : "success", "msg" : resp.text}
      else:
          print "Unable to update path" 
          response = {"result" : "failure", "msg" : resp.text}
    except Exception,e:
      print(e)
      response = {"result" : "error", "msg" : str(e)}
 
    return jsonify(response)

@app.route('/create')
def create():
    response = {"result" : "error", "msg" : "NA"}

    with open(ABS_PATH+"/create_pe1_p1_pe2_pce_xml", "r") as file:
        data = file.read()
    try:
      resp = requests.post(CREATE_PCE_PE1_P1_P2_PE2_URL, data=data, headers=HEADERS)
      if resp.status_code == 200:
          print "Path create successful!"
          response = {"result" : "success", "msg" : resp.text}
      else:
          print "Unable to create path"
          response = {"result" : "failure", "msg" : resp.text}
    except Exception,e:
      print(e)
      response = {"result" : "error", "msg" : str(e)}

    return jsonify(response)

@app.route('/delete')
def delete():
    response = {"result" : "error", "msg" : "NA"}

    with open(ABS_PATH+"/delete_pe1_pe2_pce_xml", "r") as file:
        data = file.read()
    try:
      resp = requests.post(DELETE_PCE_PE1_PE2_URL, data=data, headers=HEADERS)
      if resp.status_code == 200:
          print "Path delete successful!"
          response = {"result" : "success", "msg" : resp.text}
      else:
          print "Unable to delete path"
          response = {"result" : "failure", "msg" : resp.text}
    except Exception,e:
      print(e)
      response = {"result" : "error", "msg" : str(e)}

    return jsonify(response)

@app.route('/get')
def get():
    response = {"result" : "error", "msg" : "NA"}

    try:
      resp = requests.get(GET_PCE_URL)
      if resp.status_code == 200:
          print "Path GET successful!"
          response = {"result" : "success", "msg" : json.loads(resp.text)}
      else:
          print "Unable to GET  path"
          response = {"result" : "failure", "msg" : json.loads(resp.text)}
    except Exception,e:
      print(e)
      response = {"result" : "error", "msg" : str(e)}

    return jsonify(response)


@app.route('/reset')
def reset():
    #In sequence: delete, then create LSP.
    response = {"result" : "error", "del_msg" : "NA", "create_msg" : "NA"}
    del_flag = False
    skip_del = False
    check_state = get()
    try:
        json.loads(check_state.response[0])["msg"]["topology"][0]['node'][0]['network-topology-pcep:path-computation-client']['reported-lsp']
    except Exception, e:
        print(e)
        print "\n Skip delete, simply create the LSP"
        skip_del = True
        del_flag = True
        
    if not skip_del:
      try:
          resp = delete() 
          if resp.status_code == 200:
              print "Path Delete successful!"
              response["result"] = "success"
              response["del_msg"] = json.loads(resp.response[0])['msg']
              del_flag = True 
          else:
              print "Unable to Delete  path"
              response["result"] = "failure"
              response["del_msg"] = json.loads(resp.response[0])['msg']
      except Exception,e:
          print(e)
          response = {"result" : "del_error", "del_msg" : str(e), "create_msg" : "NA"}

    if del_flag:
        try:
            resp = create()
            if resp.status_code == 200:
                print "Path Create successful!"
                response["result"] = "success"
                response["create_msg"] = json.loads(resp.response[0])['msg']
            else:
                print "Unable to Delete  path"
                response["result"] = "failure"
                response["create_msg"] = json.loads(resp.response[0])['msg']
        except Exception,e:
            print(e)
            response["result"] = "create_error"
            response["create_msg"] = str(e)

    
    return jsonify(response)

@app.route('/switchover')
def switchover():
    #In sequence: delete, then create LSP.
    response = {"result" : "error", "msg" : "NA"}

    ero_length = ""

    check_state = get()
    try:
        ero_length = len(json.loads(check_state.response[0])["msg"]["topology"][0]['node'][0]['network-topology-pcep:path-computation-client']['reported-lsp'][0]['path'][0]['ero']['subobject'])
    except Exception, e:
        print(e)
        response = {"result" : "ero_length_error", "msg" : str(e)}

    if ero_length == 1:
      try:
          response = reset().response[0] 
      except Exception,e:
          print(e)
          response = {"result" : "error", "msg" : str(e)}
    elif ero_length == 3:
      try:
          response = update().response[0]
      except Exception,e:
          print(e)
          response = {"result" : "error", "msg" : str(e)}

    return jsonify(json.loads(response))


if __name__ == '__main__':
    register()
    app.run(host='0.0.0.0',port=6302, debug=True, use_reloader=False)
