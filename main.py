import numpy as np
import pandas as pd
import time
import random 
import math 
import threading
from concurrent import futures
import queue
import matplotlib.pyplot as plt
import seaborn as sns


class Client:

    def __init__(self, arrival_time, identifier):
        self.id = identifier
        self.arrival = arrival_time
        self.tec = None
        self.free_time = None
        self.begin_operator = None
        self.end_operator = None
        self.queue_size = None
        self.operator = None


class Controler:

    def __init__(self):

        random.seed()

        self.system_time = None
        self.system_end_time = None
        self.time_bound = 0
        self.speed = 1
        self.client_count = 0
        self.operators = []
        self.arrival_queue = queue.Queue()
        self.arrival_data = []
        self.exit_clients = []
        self.tes = []
        self.threads = None
        self.arrival_on = False
        self.operators_on = False
        self.stoped_system = False
        self.free_op = 0
        
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
            return self.lambda_distrib(self.arrival_params[0])
        elif self.arrival_distrib == 1:
            return self.normal_distrib(self.arrival_params[0], self.arrival_params[1])
        else:
            return self.uniform(self.arrival_params[0])
    def get_operator_time(self):
        if self.operator_distrib == 0:
            return self.lambda_distrib(self.operator_params[0])
        elif self.operator_distrib == 1:
            return self.normal_distrib(self.operator_params[0], self.operator_params[1])
        else:
            return self.uniform(self.operator_params[0])

    
    def arrivals(self):
      
      while self.arrival_on:
        tec = self.get_arrival_time()
        time.sleep(tec)
        localtime = time.time() - self.system_time
        client = Client(localtime, self.client_count)
        client.tec = tec
        queue_len = len(self.arrival_data)
        client.queue_size = (queue_len + 1) if self.free_op > (len(self.operators)-1) else queue_len
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

        while self.arrival_queue.qsize() > 0:
            time.sleep(0.3)

        self.operators_on = False
        self.system_end_time = time.time() - self.system_time
        self.stoped_system = True
        time.sleep(5)
        self.threads.shutdown(wait=False)
        return self.report_data()

    
    
    def report_data(self):

        if not self.stoped_system:
            return 'ERROR - SYSTEM STILL WORKING'
        begin_time = 0
        data = {'client_id':list(),'arrival_time':list(),'tec':list(),'begin_time':list(),'end_time':list(),
        'queue_time':list(), 'service_time':list(), 'free_time':list(), 'queue_size':list(), 'operator_id':list()}
        operator_data = None

        for client in self.exit_clients :
            data['client_id'].append(client.id)
            data['arrival_time'].append(float('%.2f'%client.arrival))
            data['tec'].append(float('%.2f'%client.tec))
            data['begin_time'].append(float('%.2f'%client.begin_operator))
            data['end_time'].append(float('%.2f'%client.end_operator))
            data['queue_time'].append(float('%.2f'%(client.begin_operator - client.arrival)))
            data['service_time'].append(float('%.2f'%(client.end_operator - client.begin_operator)))
            data['free_time'].append(float('%.2f'%client.free_time))
            data['queue_size'].append(float('%.2f'%client.queue_size))
            data['operator_id'].append(float('%.2f'%client.operator))
 
            begin_time = client.end_operator

        data = pd.DataFrame(data)
        
            
            
        
        return data


            


class Operator:
    
    def __init__(self, identifier, controler):
        self.id = identifier
        self.current = None
        self.controler = controler

    def operator(self):

        while self.controler.operators_on:
          free_time = time.time()
          client = self.controler.arrival_queue.get()
          self.controler.free_op += 1
          free_time = float('%.1f'%(time.time() - free_time))
          self.controler.arrival_data.pop(0)
          self.current = client.id
          print('operator ', self.id, ' started ', client.id)
          begin_time = time.time()
          client.begin_operator = begin_time - self.controler.system_time
          end_time = self.controler.get_operator_time()
          time.sleep(end_time / self.controler.speed)
          client.end_operator = client.begin_operator + end_time
          print('operator ', self.id, ' finished ', client.id)
          client.free_time = free_time
          client.operator = self.id
          self.controler.exit_clients.append(client)
          self.current = None
          free_time = 0
          self.controler.free_op -= 1

  



def get_distrib():
    print('0 - Exponential Distribution')
    print('1 - Normal Distribution')
    print('2 - Uniform Distribution')
    print('Obs: the queue theory, works only for exponential distribution')
    choosen = int(input('Type the number of the choosen distribution:'))
    if choosen < 0 or choosen > 2:
        print('Unavailable option, try again')
        get_distrib()
    else:
        if choosen == 0 or choosen == 2:
            print('I need some parameters. Can you please insert the mean?')
            mean = float(input('Mean:'))
            std = 0
        else:
            print('I need some parameters. Can you please insert the mean and the standart deviation?')
            mean = float(input('Mean:'))
            std = float(input('Standart Deviation:'))
        return choosen,[mean,std]


def menu_options():
    print('OPTIONS:')
    print('0 - Show me the data')
    print('1 - Show me the statistics for the last simulation')
    print('2 - Show me the distribution graphics')
    print('3 - Show me more graphics')
    print('4 - Save on a file')
    print('5 - Compare Results ')
    print('6 - New Simulation')
    print('7 - Quit')
    opt = int(input('option:'))
    if opt >= 0 and opt <= 7:
        return opt
    else:
        print('Invalid option, please try again...')
        menu_options()



def get_stats(stats,data):
    stats['queue_size'].append(float('%.2f'%data['queue_size'].mean()))
    stats['tec'].append(float('%.2f'%data['tec'].mean()))
    stats['tes'].append(float('%.2f'%data['service_time'].mean()))
    stats['system_time'].append(float('%.2f'%(data['service_time'] + data['queue_time']).mean()))
    stats['op_free'].append(float('%.2f'%data['free_time'].mean()))
    stats['queue_time'].append(float('%.2f'%data['queue_time'].mean()))
    sample = len(data) // 3
    total = 0
    sort_data = data.sort_values('client_id')
    total = (sort_data['queue_size'].iloc[:sample].sum() + (sample - len(sort_data.iloc[:sample][sort_data['free_time'] > 0]))*2) 
    total = total + (sort_data['queue_size'].iloc[-sample:].sum() + (sample - len(sort_data.iloc[-sample:][sort_data['free_time'] > 0]))*2) 
    total = total + (sort_data['queue_size'].iloc[sample:-sample].sum() + (sample - len(sort_data.iloc[sample:-sample][sort_data['free_time'] > 0]))*2) 
    total = float('%.2f'%(total / 3))
    #stats['system_people'].append(total)

def theorical_stats(lmbd,mi,s):
    
    stats = {}
    ro = lmbd / (mi * s)
    som = 0
    for i in range(0,s):
        som += (((s * ro) ** i) / math.factorial(i))
    term2 = (((s * ro) ** s)/ (math.factorial(s)*(1-ro)))

    pi0 = 1 / (som + term2)
    pjs = term2 * pi0

    lq = (pjs * ro) / (1 - ro)
    wq = lq / lmbd

    l = lq + (lmbd / mi)
    w = l / lmbd

    stats['queue_size'] = lq
    stats['system_people'] = l
    stats['queue_time'] = wq
    stats['system_time'] = w

    return stats

def show_stats(opt,stats):
    pass

if __name__ == '__main__':
     
     full_data = []
     stats = {'queue_size': list(), 'tec': list(), 'tes': list(), 'system_time':list(), 'op_free': list(),
     'queue_time': list()} #'system_people':list()
     quit = False
     print('Welcome to queue system simulator!')
     print('For start, lets choose few parameters...')
     print('Please, insert the number of operators')
     num_op = int(input('Number of operators:'))
     while not quit:
        while True:
            print('Now, choose the arrival distribution in clients per hour')
            lambdaa = get_distrib()
            print('Okay, lets move on...')
            print('Now, choose the service distribution in services per hour')
            mi = get_distrib()
            break
            if (lambdaa[1][0] / (mi[1][0] * num_op)) < 1:
                break
            else:
                print('The given rates violate the queue theory, where the arrival rate divided by service rate should not be greater than 1')
        lmbd = lambdaa[1][0] / 60
        mii = mi[1][0] / 60
        lambdaa[1][0] = 1 / lmbd
        mi[1][0] = 1 / mii
        
        print('Now, for last but not least important, how many minutes of simulation you would like?')
        simulation_time = float(input('Simulation time:'))
        print('Wait a second... IMPORTANT QUESTION:')
        print('Would you like to do this experiment how many times?')
        repeat = int(input('How many times should I do this experiment:'))
        for i in range(repeat):
            print('Interaction ', i+1)
            controler = Controler()
            controler.start(lambdaa[0],lambdaa[1],mi[0],mi[1],num_op)
            t_stats = theorical_stats(lmbd,mii,num_op)
            time.sleep(simulation_time)
            print('stopping')
            data = controler.stop_system()
            full_data.append(data)
            get_stats(stats,data)
            
        print('I have your results, what do you like to do next?')
        while True:
            opt = menu_options()
            if opt == 0:
                print(data)
            elif opt == 1:
                
                print('Some stats for you:')
                #print('Average clients on system(L):', stats['system_people'][-1])
                print('Theorical clients on system(L)', t_stats['system_people'])
                print('----------------------------------------------------------------------')
                print('Average clients on queue(Lq):', data['queue_size'].mean())
                print('Theorical clients on queue(Lq):', t_stats['queue_size'])
                print('----------------------------------------------------------------------')
                print('Average time spent on system(W):',(data['service_time'] + data['queue_time']).mean())
                print('Theorical time spent on system(W):', t_stats['system_time'])
                print('----------------------------------------------------------------------')
                print('Average time spent on queue(Wq):', data['queue_time'].mean())
                print('Theorical time spent on queue(Wq):', t_stats['queue_time'])
                print('----------------------------------------------------------------------')
                print('Average clients arrivals for minute:',data['tec'].mean())
                print('Theorical arrivals for minute:', 1 / lmbd )
                print('----------------------------------------------------------------------')
                print('Average services for minute:', data['service_time'].mean())
                print('Theorical services for minute:', 1 / mii)
                print('----------------------------------------------------------------------')
                print('Other stats:')
                print('Average operator free time:', data['free_time'].mean())

                
            elif opt == 2:
                sns.distplot(a=data['tec'], label='tec',kde=False)
                sns.distplot(a=data['service_time'], label='tes', kde=False)
                plt.show()
            elif opt == 3:
                print('I`m crazzy about graphics')
                sns.kdeplot(data=data['tec'], label='tec', shade=True)
                sns.kdeplot(data=data['service_time'], label='tes', shade=True)
                plt.show()
            elif opt == 4:
                print('Which name you would like for your csv file?')
                name = input('Type the name:')
                data.to_csv('./'+name+'.csv')
                print('Save sucessfully on your current directory')
            elif opt == 5:
                if len(full_data) < 2 :
                    print('There is nothing to compare mate')
                else:
                    print('Making nice comparisons to my friend...')
                    stats_data = pd.DataFrame(stats)
                    print('Some stats for you:')
                    #print('Average clients on system(L):', stats_data['system_people'].mean())
                    print('Theorical clients on system(L)', t_stats['system_people'])
                    print('----------------------------------------------------------------------')
                    print('Average clients on queue(Lq):', stats_data['queue_size'].mean())
                    print('Theorical clients on queue(Lq):', t_stats['queue_size'])
                    print('----------------------------------------------------------------------')
                    print('Average time spent on system(W):',(stats_data['tes'] + stats_data['queue_time']).mean())
                    print('Theorical time spent on system(W):', t_stats['system_time'])
                    print('----------------------------------------------------------------------')
                    print('Average time spent on queue(Wq):', stats_data['queue_time'].mean())
                    print('Theorical time spent on queue(Wq):', t_stats['queue_time'])
                    print('----------------------------------------------------------------------')
                    print('Average clients arrivals for minute:',stats_data['tec'].mean())
                    print('Theorical arrivals for minute:', 1 / lmbd )
                    print('----------------------------------------------------------------------')
                    print('Average services for minute:', stats_data['tes'].mean())
                    print('Theorical services for minute:', 1 / mii)
                    print('----------------------------------------------------------------------')
                    print('Other stats:')
                    print('Average operator free time:', stats_data['op_free'].mean())
                    
                    print(stats_data)
                    input()
                    for label in stats_data.columns:
                        sns.lineplot(label=label,data=stats_data[label])
                    plt.title('Variables average in each iteraction')
                    plt.show()
            elif opt == 6:
                full_data = []
                controler = Controler()
                print('Choose the number of operators...')
                num_op = int(input('Number of operators:'))
                break
            elif opt == 7:
                print('Okay... Hope to see you another time. Bye!')
                quit = True
                break
                
            
     

     

     
   


        