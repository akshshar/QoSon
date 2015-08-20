#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import time
import urllib2
#import controller
import subprocess

ODL_APP_URL = "http://localhost:6302"
PEER_ID = "10.2.0.3"
SAMPLE_INTERVAL = 5
TARGET_BPS = 5.2 * 1024 * 1024 

NETWORK_MONITORING_CMD ="iperf -c %s -t %d -i %d -u -y C" % \
                        (PEER_ID, SAMPLE_INTERVAL, SAMPLE_INTERVAL)


class Netmon(object):
    def __init__(self, param_dict):
        self.bw_thres = 0
        self.jitter_thres = 0 
        self.pkt_loss_thres = 0
    
        try:
            self.bw_thres = param_dict['bandwidth']
            self.jitter_thres = param_dict['jitter']
            self.pkt_loss_thres = param_dict['pktloss']
        except Exception as e:
            print e

        #Assume that the network requirements are met until tested
        self.verdict = 0
        #BW in Kbps
        self.bw = self.bw_thres 
        #Jitter in ms 
        self.jitter = self.jitter_thres
        #Packet loss in % 
        self.pkt_loss = self.pkt_loss_thres
 
    def launch_tool(self, tool):
        toolselect = {
            'iperf' : self.iperf,
            'streaming_telemetry' : self.streaming_telemetry,
            'netflow_ip_sla' : self.netflow_ip_sla,
        }

        func = toolselect.get(tool, lambda: "invalid tool")

        # Return value is a dict of network parameter values obtained via the tool, along with the verdict
        return func() 

    def iperf(self):
        # The destination parameter for iperf client may be provided by the app itself.
        # For now, we assume a configuration management tool sets up the environment variables.

#        if not os.getenv("IPERF_SERVER"):
#            print "Expecting IPERF_SERVER in the environment"
#            sys.exit(1);

#        server = os.getenv('IPERF_SERVER')
        server="10.2.0.3"
        if os.getenv("IPERF_INTERVAL"):
            interval = os.getenv("IPERF_INTERVAL")
        else:
            interval = SAMPLE_INTERVAL
       
        cmd ="iperf -c %s -t %d -i %d -u -y C" % \
                        (server, interval, interval)

        try:
            # Perform the network monitoring task 
            p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, 
                                 stderr=subprocess.PIPE)
            out, err = p.communicate()
            print out
        except Exception as e:
            print "Failed to perform the network monitoring task"
            print e
            sys.exit(1)

        #Parse the output
        
        transferred_bytes = float(out.splitlines()[2].split(',')[7])
        bps = (transferred_bytes * 8) / float(interval)

        self.bw = bps/1024.0
        self.jitter = out.splitlines()[2].split(',')[9]
        self.pkt_loss = out.splitlines()[2].split(',')[12] 
        
        return self.construct_result() 
    
    def streaming_telemetry(self):
        #TODO 
        return self.construct_result()    

    def netflow_ip_sla(self):
        #TODO
        return self.construct_result()

    def construct_result(self):
        result = {"bandwidth_result" : self.bw,
                  "jitter_result" : self.jitter,
                  "pktloss_result" : self.pkt_loss}
        print result
        #Determine the verdict
        verdict = any([float(self.bw) < float(self.bw_thres), self.jitter > self.jitter_thres, self.pkt_loss > self.pkt_loss_thres])
        print "verdict is"
        print verdict
        self.verdict = str(verdict == True)
        result['verdict'] = self.verdict

        return result
    
