#!/usr/bin/env python

# author: akshshar 
# https://github.com/akshshar/qoson

import os, sys, time, pdb, Queue, requests, threading 
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
import netmon


class registerData():
    def __init__(self, name, url, method, data, header, role):
        self.name = name 
        self.url = url 
        self.method = method 
        self.data = data 
        self.header = header 
        self.role = role

class appData():
    def __init__(self, app_id, tool, nw_resources):
        self.app_id = app_id
        self.taskData = {}
        self.slaves = [] 
        self.tool = tool 
        self.param_dict = nw_resources 

    def fillTaskData(self, task_id, net_rack, role):
        self.taskData[task_id] = (net_rack, role)


class NetmonScheduler(mesos.interface.Scheduler):
    def __init__(self, implicitAcknowledgements, executor, cpu_alloc, mem_alloc, refuse_seconds, interval=1):
        self.implicitAcknowledgements = implicitAcknowledgements
        self.executor = executor
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.messagesSent = 0
        self.messagesReceived = 0
        self.appQueue = Queue.Queue() 
        self.appDatadict = {}
        # resource requirements
        self._cpu_alloc = cpu_alloc
        self._mem_alloc = mem_alloc
        self._offer_timeout = mesos_pb2.Filters()
        self._offer_timeout.refuse_seconds = refuse_seconds 
        self.clientQueue = Queue.Queue() 
        self.clientDict = {}

        self.interval = interval
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    def run(self):
        """ Method that runs forever to accept clients"""
        while True:
            if not self.clientQueue.empty():
                clientObj= self.clientQueue.get()   
                self.clientDict[clientObj.role] = clientObj 
            time.sleep(self.interval)


    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value


    def resourceOffers(self, driver, offers):
        appDatarcvd = None 
       # Pop the last app_id and launch a monitoring task on  every valid slave                                                                                                          

        if not self.appQueue.empty():                                                                                                                                           
            appDatarcvd = self.appQueue.get()                                                                                                                                   
        for offer in offers:
            tasks = []
            offerCpus = 0
            offerMem = 0
            offerAttributes = 0
            isMonitoringDevice = False 
            role = ""
            rack = ""
 
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value

            for attribute in offer.attributes:
                print "Received attribute with name: %s, value: %s" \
                    % (attribute.name, attribute.text.value)

                if attribute.name == "device-type" and attribute.text.value =="network":
                    isMonitoringDevice = True

                if attribute.name == "device-role":
                    role = attribute.text.value
                    
                if attribute.name == "rack":             
                    rack = attribute.text.value
           
            print "Received offer %s with cpus: %s and mem: %s" \
                  % (offer.id.value, offerCpus, offerMem)


            if isMonitoringDevice == 1:
                launch_task = 0

                if appDatarcvd != None:
                    if  appDatarcvd.app_id in self.appDatadict:
                        if offer.slave_id.value not in self.appDatadict[appDatarcvd.app_id].slaves:
                            # launch monitoring tasks for apps uniquely on individual slaves#
                            launch_task = 1
                            self.appDatadict[appDatarcvd.app_id].slaves.append(offer.slave_id.value)
                    else:
                        self.appDatadict[appDatarcvd.app_id] = appDatarcvd
                        launch_task = 1
                        self.appDatadict[appDatarcvd.app_id].slaves.append(offer.slave_id.value)


                    tid = str(self.tasksLaunched)
                    self.tasksLaunched += 1
                    print "Netmon Scheduler: accepting offer on slave %s to start task %s" % (offer.hostname, tid)
                    task = mesos_pb2.TaskInfo()
                    task.task_id.value = str(tid)

                    #Populate task information in appDatadict
                    self.appDatadict[appDatarcvd.app_id].fillTaskData(tid, rack, role)
                 
                    task.labels.labels.add(key = "app_id", value = appDatarcvd.app_id)
                    task.labels.labels.add(key = "tool", value = appDatarcvd.tool)
                    for param in appDatarcvd.param_dict:
                        task.labels.labels.add(key = param, value = appDatarcvd.param_dict[param])

                    task.slave_id.value = offer.slave_id.value
                    task.name = "monitoring task %s" % tid
                    task.executor.MergeFrom(self.executor)


                    cpus = task.resources.add()
                    cpus.name = "cpus"
                    cpus.type = mesos_pb2.Value.SCALAR
                    cpus.scalar.value = self._cpu_alloc 

                    mem = task.resources.add()
                    mem.name = "mem"
                    mem.type = mesos_pb2.Value.SCALAR
                    mem.scalar.value = self._mem_alloc 

                    tasks.append(task)

                    if mem.scalar.value <= offerCpus and mem.scalar.value <= offerMem: 
                        driver.launchTasks([offer.id], tasks, self._offer_timeout)

            driver.declineOffer(offer.id, self._offer_timeout)
 


    def statusUpdate(self, driver, update):
        print "Task %s is in state %s" % \
            (update.task_id.value, mesos_pb2.TaskState.Name(update.state))
 
        if update.state == mesos_pb2.TASK_RUNNING:
            print "Process the monitoring task verdict"
            if update.data == "True":
                app_id = update.labels.labels[0].value 
                tid = update.task_id.value
                rack, role = self.appDatadict[app_id].taskData[tid] 
                params = {'rack' : rack, 'app_id' : app_id}
                try:           
                    actionData = self.clientDict[role] 
                    if actionData.method == "GET":
                        requests.get(actionData.url, 
                                     headers=actionData.header, 
                                     params=params)       
                    if actionData.method == "POST":
                        requests.post(actionData.url,
                                      data=actionData.data,
                                      headers=actionData.header,
                                      params=params) 
                except Exception as e:
                    print "Unable to handle task verdict"
                    print e


        if update.state == mesos_pb2.TASK_FINISHED:
            app_id = update.labels.labels[0].value                                                                                                                                      
            slave = update.slave_id.value
            if slave in self.appDatadict[app_id].slaves:
                self.appDatadict[app_id].slaves.remove(slave)

        #TODO Take care of tasks that are lost or stuck in staging, do task reconciliation

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FAILED:
            print "Aborting because task %s is in unexpected state %s with message '%s'" \
                % (update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message)
            driver.abort()

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.messagesReceived += 1


    @staticmethod
    def start_framework (master_uri, exe_path, cpu_alloc, mem_alloc, refuse_seconds):

        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = "default"
        executor.command.value = exe_path
        executor.name = "QoSon Executor (Python)"
        executor.source = "network monitoring"

        framework = mesos_pb2.FrameworkInfo()
        framework.user = "" # Have Mesos fill in the current user.
        framework.name = "QoSon Framework (Python)"

        implicitAcknowledgements = 1
        if os.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS"):
            print "Enabling explicit status update acknowledgements"
            implicitAcknowledgements = 0

        # create a scheduler and capture the command line options
        sched = NetmonScheduler(implicitAcknowledgements, executor, cpu_alloc, mem_alloc, refuse_seconds)

        if os.getenv("MESOS_CHECKPOINT"):
            print "Enabling checkpoint for the framework"
            framework.checkpoint = True

        if os.getenv("MESOS_AUTHENTICATE"):
            print "Enabling authentication for the framework"

            if not os.getenv("DEFAULT_PRINCIPAL"):
                print "Expecting authentication principal in the environment"
                sys.exit(1);

            if not os.getenv("DEFAULT_SECRET"):
                print "Expecting authentication secret in the environment"
                sys.exit(1);

            credential = mesos_pb2.Credential()
            credential.principal = os.getenv("DEFAULT_PRINCIPAL")
            credential.secret = os.getenv("DEFAULT_SECRET")

            framework.principal = os.getenv("DEFAULT_PRINCIPAL")

            driver = mesos.native.MesosSchedulerDriver(
                sched,
                framework,
                master_uri,
                implicitAcknowledgements,
                credential)
        else:
            framework.principal = "test-framework-python"

            driver = mesos.native.MesosSchedulerDriver(
                sched,
                framework,
                master_uri,
                implicitAcknowledgements)

        return driver, sched

    @staticmethod
    def stop_framework (driver):
        """ensure that the driver process terminates"""
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop();
        sys.exit(status)

