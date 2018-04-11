import csv
import itertools
from collections import deque



class DAG:
    #function to initialize the graph
    def __init__(self):
        self.graph = dict()

    #function to add in_degrees to graph
    def in_degrees(self):
        in_degrees = dict()
        #traverse through the adjacency list and fill with vertices
        for node in self.graph:
            if node not in in_degrees:
                in_degrees[node] = 0
            for pointed in self.graph[node]:
                if pointed not in in_degrees:
                    in_degrees[pointed] = 0
                in_degrees[pointed] += 1
        return in_degrees

    #function to topological sort based on kahn's algorithm 
    def sort(self):
        in_degrees = self.in_degrees()
        visit = deque() #deque all nodes
        for node in self.graph:
            if in_degrees[node] == 0:
                visit.append(node)

        queue = list() #create list to store the result
        while visit:
            #extract in front of visit and add it to queue
            node = visit.popleft()
            queue.append(node)
            #iterate through all neighboring nodes and decrease their in-degree by 1
            for pointer in self.graph[node]:
                in_degrees[pointer] -= 1
                if in_degrees[pointer] == 0: #if degree is 0, add it to queue
                    visit.append(pointer)
        return queue

    #function to add nodes to graph
    def add(self, node, to=None):
        if node not in self.graph:
            self.graph[node] = list()
        if to:
            if to not in self.graph:
                self.graph[to] = list()
            self.graph[node].append(to)
        if len(self.sort()) != len(self.graph): #check if there's a cycle
            raise Exception


class Pipeline:
    #function to initialize pipeline
    def __init__(self):
        self.tasks = DAG()

    #function to add tasks
    def task(self, depends_on=None):
        def inner(f):
            self.tasks.add(f)
            if depends_on: 
                self.tasks.add(depends_on, f)
            return f
        
        return inner
    #function to run tasks
    def run(self):
        sortedtask = self.tasks.sort()
        completed = dict()
        #iterate through topological sort and adds tasks to dictionary
        for task in sortedtask:
            for node, values in self.tasks.graph.items():
                if task in values: 
                    completed[task] = task(completed[node])
            if task not in completed:
                completed[task] = task()
        return completed

#function to output csv files
def build_csv(lines, header=None, file=None):
    #add header
    if header:
        lines = itertools.chain([header], lines)
    writer = csv.writer(file, delimiter=',')
    writer.writerows(lines)
    file.seek(0)
    return file
