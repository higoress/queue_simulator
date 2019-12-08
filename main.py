import numpy as np
import pandas as pd
import time
import random 
import math 
import threading
from concurrent import futures
import queue



class Client:

    def __init__(self, arrival_time, identifier):
        self.id = identifier
        self.arrival = arrival_time
        self.begin_operator = None
        self.end_operator = None


class Controler:

    def __init__(self):

        random.seed()

        self.system_time = None
        self.time_bound = 0
        self.speed = 1
        self.client_count = 0
        self.operators = []
        self.arrival_queue = queue.Queue()
        self.arrival_data = []
        self.threads = None
        self.arrival_on = False
        self.operators_on = False
        
        self.arrival_distrib = None
        self.arrival_params = None
        self.operator_distrib = None
        self.operator_params = None
        



    def start(self, arrival_distrib, arrival_params, operator_distrib, operator_params, num_operators=1):
      
      self.arrival_distrib = arrival_distrib
      self.arrival_params = arrival_params
      self.operator_distrib = operator_distrib
      self.operator_params = operator_params
      self.arrival_on = True
      self.operators_on = True

      for i in range(num_operators):
          #print('Added operator ', i)
          self.operators.append(Operator(i,self))
      workers = 2 + num_operators
      self.threads = futures.ThreadPoolExecutor(max_workers=workers)
      
      self.system_time = time.time()
      self.threads.submit(self.arrivals)

      print('start system')
      for i in range(num_operators):
          print('start operator', i)
          self.threads.submit(self.operators[i].operator)

    def lambda_distrib(self,mean):
        if mean != 0 :
            lambd = 1 / mean
            return abs(math.log(1 - random.random()) / lambd)

    def normal_distrib(self, mean, std):
        s = 0
        while s >= 1:
            u1 = random.random()
            u2 = random.random()

            v1 = (2 * u1) - 1
            v2 = (2 * u2) - 1

            s = (v1 * v1) + (v2 * v2)

        multiplier = math.sqrt(-2 * math.log(s) / s)

        x1 = multiplier * v1
        x2 = multiplier * v2

        return abs((std * x1)+ mean)


    def uniform(self, mean):
        value = 2 * mean * random()
        return value

    def get_arrival_time(self):
        if self.arrival_distrib == 0:
            return self.lambda_distrib(self.arrival_params)
        elif self.arrival_distrib == 1:
            return self.normal_distrib(self.arrival_params[0], self.arrival_params[1])
        else:
            return self.uniform(self.arrival_params)
    def get_operator_time(self):
        if self.operator_distrib == 0:
            return self.lambda_distrib(self.operator_params)
        elif self.operator_distrib == 1:
            return self.normal_distrib(self.operator_params[0], self.operator_params[1])
        else:
            return self.uniform(self.operator_params)

    
    def arrivals(self):
      
      while self.arrival_on:
        time.sleep(self.get_arrival_time())
        localtime = time.time()
        localtime = localtime - self.system_time
        client = Client(localtime, self.client_count)
        print('Client ', self.client_count, 'join the system')
        self.client_count += 1
        self.arrival_data.append(client.id)
        self.arrival_queue.put(client)

    def get_status(self):
        data = {}
        data['total_clients'] = self.arrival_queue.qsize()
        data['list_clients_id'] = self.arrival_data
        for operator in self.operators:
            data['operator' + str(operator.id)] = operator.current if operator.current != None else 0
        
        return data

    def stop_system(self):
        
        self.arrival_on = False

        while self.arrival_queue.qsize > 0:
            time.sleep(0.3)

        self.operators_on = False
        



class Operator:
    
    def __init__(self, identifier, controler):
        self.id = identifier
        self.current = None
        self.exit = []
        self.controler = controler

    def operator(self):

        while self.controler.operators_on:
          client = self.controler.arrival_queue.get()
          self.controler.arrival_data.pop(0)
          self.current = client.id
          print('operator ', self.id, ' started ', client.id)
          begin_time = time.time()
          client.begin_operator = begin_time - self.controler.system_time
          end_time = self.controler.get_operator_time()
          time.sleep(end_time / self.controler.speed)
          client.end_operator = client.begin_operator + end_time
          print('operator ', self.id, ' finished ', client.id)
          self.exit.append(client)
          self.current = None

    def statistics(self):
        pass






if __name__ == '__main__':
     controler = Controler()
     controler.start(0,2,0,3.5,2)
   


        