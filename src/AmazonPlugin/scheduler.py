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
        self.worknodes = self.workloads.keys()
        self.nodeinfo = self.read_nodeinfo()
        self.graph = self.get_graph()

    def read_workflow(self):
        jsonfile=open('/Users/claudwang/.vistrails/userpackages/AMAZONPlugin/workflow.json') 
        #Change the address if you use it at your local
        # you can find the json file format in the "test" folder
        workflow = json.load(jsonfile)
        jsonfile.close()
        return workflow

    def get_graph(self):
        graph = {}
        for upnode,downnodes in self.wf.iteritems():
            for downnode in downnodes:
                graph.setdefault(downnode, [])
                graph[downnode].append(upnode)
        return graph

    def read_workload(self):
        jsonfile=open('/Users/claudwang/.vistrails/userpackages/AMAZONPlugin/workload.json')
        #Change the address if you use it at your local
        # you can find the json file format in the "test" folder
        workload = json.load(jsonfile)
        jsonfile.close()
        return workload

    def read_nodeinfo(self):
        jsonfile=open('/Users/claudwang/.vistrails/userpackages/AMAZONPlugin/nodeinfo.json')
        #Change the address if you use it at your local
        # you can find the json file format in the "test" folder
        nodeinfo = json.load(jsonfile)
        jsonfile.close()
        return nodeinfo

    def get_overall_deadline(self):
        deadline = 1000
        # test data
        return deadline
    
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

    def print_schedule(self,node_computing):
        nodeinfo = self.nodeinfo
        power_node = {}
        for node,(des,cost,power) in nodeinfo.iteritems():
            power_node.setdefault(power,node)
        print "Workflow Mapping Schedule:"
        for task,power in node_computing.iteritems():
            node = power_node[power]
            print "%s ---> %s"%(task,node)
            print "Esimatied Cost: $%d"%(nodeinfo[node][1])
        return 0
    
    def get_computing_power(self):
        nodeinfo = self.nodeinfo
        # u'Harpertown': [u'3', 600, 100]
        computing_power = {}
        for node,(description,price,power) in nodeinfo.iteritems():
            computing_power.setdefault(power,node)
        return computing_power

    def get_deadline(self,node,timecosts):
        deadline = timecosts[node]
        if self.graph.has_key(node):
            longest = 0
            for upnode in self.graph[node]:
                node_deadline = self.get_deadline(upnode,timecosts)
                if longest<node_deadline:
                    longest = node_deadline
            deadline += longest
        else:
            return deadline
        return deadline

    def find_startnode(self,graph):
        for node in self.worknodes:
            if not graph.has_key(node):
                endnode = node
        return endnode

    def find_endnode(self,graph):
        for node in self.worknodes:
            if len(self.wf[node])==0:
                endnode = node
        return endnode

    def pickup_computing(self,bottom_line,powers):
        for power in powers:
            if power > bottom_line:
                return power
        return powers[-1]


    def schedule(self):
        graph = self.graph
        workloads = self.workloads
        computing_power = self.get_computing_power()
        powers = sorted(computing_power.keys())
        overall_deadline = self.get_overall_deadline()
        
        # Find the end node
        startnode = self.find_startnode(graph)
        endnode = self.find_endnode(graph)

        end_deadline = 99999999 # A number big enough
        computing = powers[0]
        
        # Get the inial time cost
        timecosts = {}
        node_computing = {}
        for node,workload in workloads.iteritems():
            timecosts.setdefault(node,workload/float(computing))
            node_computing.setdefault(node,computing)
        deadlines = {}

        # All node will have an intial deadline
        for node in self.wf:
            deadlines.setdefault(node,self.get_deadline(node,timecosts))
        end_deadline = max(deadlines.values())

        # Iteration update deadlines to match the final deadline
        num = 0
        while end_deadline > overall_deadline:
            # Find the longest path
            in_path = []
            checknode = endnode
            in_path.append(checknode)
            while checknode != startnode:
                deadline = 0
                candidate = 0
                for upnode in graph[checknode]:
                    if deadlines[upnode] > deadline:
                        candidate = upnode
                        deadline = deadlines[upnode]
                checknode = candidate
                in_path.append(checknode)
            total_work_in_path = 0
            for node in in_path:
                total_work_in_path += workloads[node]
            bottom_line = total_work_in_path/float(overall_deadline)
            computing = self.pickup_computing(bottom_line,powers)
            for node in in_path:
                node_computing[node] = computing
                timecosts[node] = self.workloads[node]/float(computing) 
            for node in in_path:
                deadlines[node] = self.get_deadline(node,timecosts)
            end_deadline = max(deadlines.values())
            num += 1

        # return the plan 
        print ""
        print "Total time estimated: %s minutes"%end_deadline
        print ""
        return node_computing

def main():
    scheduler = Scheduler()
    node_computing = scheduler.schedule() 
    scheduler.print_schedule(node_computing)

if  __name__ =='__main__':
    main()
