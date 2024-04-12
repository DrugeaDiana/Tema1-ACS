from queue import Queue
from threading import Thread, Event, BoundedSemaphore
import os
import time
import multiprocessing
import json
import logging
from logging.handlers import RotatingFileHandler
class Job:
    def __init__(self, id, type, data_ingestor):
        self.id = id
        self.type = type
        self.result = None
        self.state = None
        self.status = 'Pending'
        self.data_ingestor = data_ingestor

    def set_question(self, question):
        self.question = question

    def set_state(self, state):
        self.state = state

    def execute(self):
        self.status = 'Running'
        #self.semaphore = sem
        print(self.type)
        match self.type:
            case 'states_mean':
                self.states_mean()
            case 'state_mean':
                self.state_mean()
            case 'best5':
                self.best5()
            case 'worst5':
                self.worst5()
            case 'global_mean':
                self.global_mean()
            case 'diff_from_mean':
                self.diff_from_mean()
            case 'state_diff_from_mean':
                self.state_diff_from_mean()
            case 'mean_by_category':
                self.mean_by_category()
            case 'state_mean_by_category':
                self.state_mean_by_category()
            case _:
                return 0
        self.status = 'Done'
    def calculate_all_means(self):
        cols = ["Question", "LocationDesc"]
        col = 'Question'
        rez = self.data_ingestor.data.loc[self.data_ingestor.data[col]
                       == self.question].groupby(cols)['Data_Value'].mean()
        return rez
    
    def turn_result_in_dict(self, rez, value = -1):
        rez = rez.to_dict()
        values_dict = dict()
        if value == -1:
            for key in rez.keys():
                values_dict[key[1]] = rez[key]
        else:
            for key in rez.keys():
                values_dict[key[1]] =  value - rez[key]
        return values_dict

    def states_mean(self):
        rez = self.calculate_all_means()
        rez = rez.sort_values()
        self.result = self.turn_result_in_dict(rez)

    def calculate_state_mean(self, state):
        cols = ["Question", "LocationDesc"]
        rez = self.data_ingestor.data.groupby(cols)
        rez = rez.get_group((self.question, state))['Data_Value'].mean()
        return rez

    def state_mean(self):
        rez = self.calculate_state_mean(self.state)
        self.result = {self.state : rez}


    
    def best5(self):
        rez = self.calculate_all_means()
        if self.question in self.data_ingestor.questions_best_is_max:
            rez = rez.sort_values(ascending=False)
        else:
            rez = rez.sort_values()
        sorted_dict = dict()
        rez = rez.to_dict()
        counter = 0
        for key in rez.keys():
            sorted_dict[key[1]] = rez[key]
            counter += 1
            if counter == 5:
                break

        self.result = sorted_dict

    def worst5(self):
        rez = self.calculate_all_means()
        if self.question in self.data_ingestor.questions_best_is_max:
            rez = rez.sort_values()
        else:
            rez = rez.sort_values(ascending = False)

        sorted_dict = dict()
        rez = rez.to_dict()
        counter = 0
        for key in rez.keys():
            sorted_dict[key[1]] = rez[key]
            counter += 1
            if counter == 5:
                break
        self.result = sorted_dict

    def global_mean(self):
        # Add synch here

        # end synch

        rez = self.data_ingestor.data.groupby(['Question'])
        rez = rez.get_group((self.question,))['Data_Value'].mean()
        rez_dict = dict()
        rez_dict["global_mean"] = rez
        self.result = rez_dict
        # add synch here
        # end synch
        return rez
        #print(self.question, " ", rez)
    
    
    def state_diff_from_mean(self):
        state_mean = self.calculate_state_mean(self.state)
        
        global_mean = self.global_mean()
        rez = global_mean - state_mean
        self.result = {self.state : rez }
        return rez
    
    def diff_from_mean(self):
        global_mean = self.global_mean()
        rez = self.calculate_all_means()
        self.result = self.turn_result_in_dict(rez, global_mean)

    def mean_by_category(self):
        cols = ['Question', 'LocationDesc', 'StratificationCategory1', 'Stratification1']
        rez = self.data_ingestor.data.loc[self.data_ingestor.data['Question'] 
                                               == self.question].groupby(cols)['Data_Value'].mean()
        rez.to_dict()
        dictionary_result = dict()
        for key in rez.keys():
            key_string = "('" + key[1] + "', '" + key[2] + "', '" + key[3] + "')"
            dictionary_result[key_string] = rez[key]
        self.result = dictionary_result
    
    def state_mean_by_category(self):
        cols = ['Question', 'LocationDesc', 'StratificationCategory1', 'Stratification1']
        rez = self.data_ingestor.data[(self.data_ingestor.data['Question'] 
                                            == self.question) & 
                                            (self.data_ingestor.data['LocationDesc'] == self.state)]
        rez = rez.groupby(cols)['Data_Value'].mean()
        rez.to_dict()
        dictionary_result = dict()
        for key in rez.keys():
            key_string = "('" + key[2] + "', '" + key[3] + "')"
            dictionary_result[key_string] = rez[key]
        result = dict()
        result[self.state] = dictionary_result
        self.result = result


class ThreadPool:
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task

        self.num_threads = self._get_num_threads()
        self.tasks_queue = Queue()
        self.all_tasks = []
        self.threads = []
        self.semaphore = BoundedSemaphore(1)
        self._create_workers()
        self.active = True
        self.logger = logging.getLogger('my_logger')
        handler = RotatingFileHandler('webserver.log', maxBytes=2000, backupCount=10)
        self.logger.addHandler(handler)
    
    def submit_task(self, task):
        if self.active is True:
            self.tasks_queue.put(task)
            self.all_tasks.append(task)

    def _get_num_threads(self):
        env_var = os.getenv('TP_NUM_OF_THREADS')
        if env_var is not None:
            return int(env_var)
        else:
            return multiprocessing.cpu_count()
    
    def _create_workers(self):
        for i in range(self.num_threads):
            worker = TaskRunner(self.tasks_queue)
            worker.start()
            self.threads.append(worker)
    def wait_completion(self):
        self.tasks_queue.join()
        self.active = False
        for _ in range(self.num_threads):
            self.submit_task(None)


class TaskRunner(Thread):
    def __init__(self, task_queue):
        Thread.__init__(self)
        self.task_queue = task_queue
        #self.sem = semaphore
        
    def run(self):
        while True:
            # TODO
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            job = self.task_queue.get()
            if job is None:
                self.task_queue.task_done()
                break
            job.execute()

            self.task_queue.task_done()
            
            
