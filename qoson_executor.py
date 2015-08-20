#!/usr/bin/env python

# author: akshshar 
# https://github.com/akshshar/qoson

import sys
import threading
import time
import subprocess
import pdb
from inspect import getmembers
from pprint import pprint

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from netmon import Netmon

class NetmonExecutor(mesos.interface.Executor):
    def launchTask(self, driver, task):
        # Create a thread to run the task. Tasks should always be run in new
        # threads or processes, rather than inside launchTask itself.
        def run_task():
            print "Running task %s" % task.task_id.value


            # The list of parameters to monitor on the network device are provided via the task labels
      	    # The First task label (task.labels.labels[0]) signifies the app_ID for the app launched on Marathon
                # key = 'app_id', value = actual app_id from Marathon 
       	    # The second task label (task.labels.labels[1]) signifies the tool to be used to fetch telemetry data
                # key = 'tool', value = 'iperf', 'streaming_telemetry', 'netflow' etc.
   	        # The next set of labels (task.labels.labels[2]+) signifies the list of key-value pairs such as
    	    # Available_Bandwidth, Jitter, Packet loss etc., that a telemetry agent/receiver run by the executor can 
       	    # use as thresholds to compare and respond to.

       	    # param_dict is meant to be a dictionary representation of task.labels.labels[2]+

            param_dict = {} 
            env_tags = ['app_id', 'tool']
            
            # Create a dictionary from the free-form key-value array of task labels.
            print task.labels.labels
            for label in task.labels.labels:
                print label
                print "key is "+label.key
                print "value is "+ label.value
                param_dict[label.key] = label.value
          
            tool = param_dict['tool']
 
            # Weed out the standard environment tags
            for tag in env_tags:
                del param_dict[tag]

            # Create the netmon object to launch the monitoring task using the thresholds
            
            netmon = Netmon(param_dict)
            
            # Launch the tool and capture the result (dict with  
            # actual network parameter values and the verdict = binary value)
            result = netmon.launch_tool(tool)
     
            print "Sending status update...%s" % (result)
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.slave_id.value = task.slave_id.value
            update.state = mesos_pb2.TASK_RUNNING
            update.data = result['verdict'] 

            # update.labels.labels in the TaskStatus object will contain the original
            # key,value pair for the task, followed by the actual network parameter
            # values from result minus the verdict.

            del result['verdict']

            update.labels.labels.extend(task.labels.labels)          
            for tag in result.keys():
                update.labels.labels.add(key = tag, value = str(result[tag])) 

            driver.sendStatusUpdate(update)
            print "Sent intermediate status update"

            time.sleep(5)

            print "Sending status update..."
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            update.data = 'data with a \0 byte'
            update.labels.labels.extend(task.labels.labels)
            driver.sendStatusUpdate(update)
            print "Sent status update"

        thread = threading.Thread(target=run_task)
        thread.start()

    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        driver.sendFrameworkMessage(message)

if __name__ == "__main__":
    print "Starting executor"
    driver = mesos.native.MesosExecutorDriver(NetmonExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
