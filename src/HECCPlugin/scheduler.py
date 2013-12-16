import json
import time
import random
from pprint import pprint

class Task:
    def __init__(self,name,workload):
        self.name = name
        self.workload = workload

class Workflow:
    def __init__(self,name):
        self.name = name
        self.tasks = {}

def simulate(seconds):
    for i in range(seconds):
        print '.'
        time.sleep(0)

def print_workflow(workflow):
    print "Workflow Capture:"
    simulate(2)
    for task,to_tasks in workflow.iteritems():
        for to_task in to_tasks:
            print "%s ---> %s"%(task,to_task)
    simulate(3)
    return 0

def print_workload(workload):
    print "Workload Capture:"
    simulate(2)
    for task,workload in workload.iteritems():
        print "%s: %s"%(task,workload)
    simulate(3)
    return 0

def print_nodeinfo(nodeinfo):
    print "Compute Nodes Specification:"
    simulate(1)
    for node,info in nodeinfo.iteritems():
        print "Node: '%s'"%(node)
        print "Processor Speed: %s GHz"%(info[0])
        print "Esimatied Cost: $%d per minute"%(info[1])
        print "Esimatied performance: %d units per minute"%(info[2])
        print ""
    simulate(3)
    return 0

class Scheduler:
    def __init__(self):
        self.wf = self.read_workflow()
        self.workloads = self.read_workload()
        self.nodeinfo = self.read_nodeinfo()

    def read_workflow(self):
        jsonfile=open('/Users/claudwang/.vistrails/userpackages/HECCPlugin/workflow.json')
        workflow = json.load(jsonfile)
        #print_workflow(workflow)
        jsonfile.close()
        return workflow

    def read_workload(self):
        jsonfile=open('/Users/claudwang/.vistrails/userpackages/HECCPlugin/workload.json')
        workload = json.load(jsonfile)
        #print_workload(workload)
        jsonfile.close()
        return workload

    def read_nodeinfo(self):
        jsonfile=open('/Users/claudwang/.vistrails/userpackages/HECCPlugin/nodeinfo.json')
        nodeinfo = json.load(jsonfile)
        #print_nodeinfo(nodeinfo)
        jsonfile.close()
        return nodeinfo

    def get_overall_deadline(self):
        deadline = 1500
        return deadline

    def assign_deadline(self):
        overall_deadline = self.get_overall_deadline()
        workloads = self.workloads
        work_deadline = {}
        total_workload = 0
        for work,workload in workloads.iteritems():
            total_workload += workload
        for work,workload in workloads.iteritems():
            work_deadline.setdefault(work,overall_deadline*workload/total_workload)
        return work_deadline

    def get_nodes_info(self):
        nodes_info = { "x":(10,150),"y":(30,400),"z":(20,300) }
        return nodes_info

    def check_resource(self):
        resource_list = { "x":3,"y":4,"z":2 }
        return resource_list
   
    def partition(self):
        uow = {}
        dependency = {}
        uow.setdefault(self.wf.keys()[0],[]) # add start point
        for node,point_to in self.wf.iteritems():
            if len(point_to)!=0:
                for tnode in point_to:
                    dependency.setdefault(tnode,[])
                    dependency[tnode].append(node)
        for tnode,point_to in dependency.iteritems():
            if len(point_to)>1:
                uow.setdefault(tnode,[])   # sync node
        for tnode in uow:
            if dependency.has_key(tnode):
                dependency.pop(tnode)
        for tnode,point_to in dependency.iteritems():
            if dependency.has_key(point_to[0]):
                uow.setdefault(tnode,[])
                uow[tnode].append(point_to[0])
                uow.setdefault(point_to[0],-1)
            elif not uow.has_key(tnode):
                uow.setdefault(tnode,[])
        for tnode in uow:
            if dependency.has_key(tnode):
                dependency.pop(tnode)
        uowl = []
        for node,value in uow.iteritems():
            if value != -1:
                if len(value)==0:
                    uowl.append(node)
                else:
                    t = value
                    t.append(node)
                    uowl.append(t)
        return uowl

    def generate_node(self,updated_list,nodes_info):
        #for node,info in nodes_info.iteritems():
        #sorted(nodes_info.iteritems(),key = lambda x:x[1])[0]
        if int(random.random()*100)%4 == 0:
            return "Sandy_Bridge"
        elif int(random.random()*100)%4 == 0:
            return "Westmere"
        elif int(random.random()*100)%4 == 0:
            return "Nehalem"
        else:
            return "Harpertown"

    def schedule(self):
        work_deadline = self.assign_deadline()
        independent_task,sequence_task = self.partition()
        independent_task_node = self.assign_node(independent_task,work_deadline)
        resource_list = self.check_resource()
        nodes_info = self.get_nodes_info()
        deadline = self.get_overall_deadline()
        plan = {}
        updated_list = resource_list
        for work in uowl:
            if type(work)==list:
                for job in work:
                    plan.setdefault(job,self.assign_node(updated_list,nodes_info))
            else:
                plan.setdefault(work,self.assign_node(updated_list,nodes_info))
        return plan

    def assign_node(self, task_list, work_deadline):
        workloads = self.workloads
        task_bottomline = {}
        for task,workload in workloads.iteritems():
            task_bottomline.setdefault(task,workload/work_deadline[task])
        nodeinfo = self.nodeinfo
        task_node = {}
        for task,bottomline in task_bottomline.iteritems():
            task_node.setdefault(task,('none',10000000000))
            for node,(CPU,cost,performance) in nodeinfo.iteritems():
                if performance>bottomline & performance<task_node[task][1]:
                    node = self.generate_node([],self.nodeinfo)
                    task_node[task] = (node,performance)
        return task_node

    def print_schedule(self,schedule):
        nodeinfo = self.nodeinfo
        print "Workflow Mapping Schedule:"
        simulate(2)
        for task,(node,performance) in schedule.iteritems():
            print "%s ---> %s"%(task,node)
            print "Esimatied Time: %d minutes"%(nodeinfo[node][1])
            print "Esimatied Cost: $%d"%(nodeinfo[node][1])
            print ""
        return 0

    def get_cost(self,san,wes,neh,har):
        # SBU charged = Wall_Clock_Hours_Used * Number of MAUs * SBU Rate
        cost = 100*san*1.82+100*wes*1+100*neh*0.8+100*neh*0.45
        return cost

def main():
    scheduler = Scheduler()
    work_deadline = scheduler.assign_deadline()
    workflow = read_workflow()
    work_node = scheduler.assign_node(workflow,work_deadline)
    scheduler.print_schedule(work_node)

if  __name__ =='__main__':
    main()
