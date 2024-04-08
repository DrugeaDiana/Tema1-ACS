from queue import Queue
from threading import Thread, Event
import os
import time
import multiprocessing
from app import webserver
import more_itertools
from flask import jsonify
class Job:
    def __init__(self, id, type):
        self.id = id
        self.type = type
        self.result = None
        self.state = None
        self.status = 'Pending'
    
    def set_question(self, question):
        self.question = question

    def set_state(self, state):
        self.state = state

    def execute(self):
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
                return 1
            case 'state_diff_from_mean':
                self.state_diff_from_mean()
            case 'mean_by_category':
                return 1
            case 'state_mean_by_category':
                return 1
            case _:
                return 0

    def states_mean(self):
        sum = 0
        counter = 0
        for row in webserver.data_ingestor.dictionary_data:
            if row["Question"] == self.question:
                sum += row["Data_Value"]
                counter +=1
        rez = sum / counter
        self.result = jsonify(rez)

    def calculate_state_mean(self, state):
        sum = 0
        counter = 0
        for row in webserver.data_ingestor.dictionary_data:
            if row["Question"] == self.question and row["State"] == state:
                sum += row["Data_Value"]
                counter +=1
        rez = sum / counter
        return rez

    def state_mean(self):
        rez = self.calculate_state_mean()
        self.result = jsonify(rez)

    def calculate_all_means(self):
        dictionary_by_states = dict()
        dictionary_means = dict()
        for row in webserver.data_ingestor.dictionary_data:
            if row["Question"] == self.question:
                if row["State"] not in dictionary_by_states:
                    dictionary_by_states.update({row["State"] : {"sum" : 0, "counter" : 0}})
                else :
                    dictionary_by_states[row["State"]]["sum"] += row["Data_Value"]
                    dictionary_by_states[row["State"]]["counter"] += 1 
        for state in dictionary_by_states:
            dictionary_means.update({state : dictionary_by_states[state]["sum"] / dictionary_by_states[state]["counter"]})
        return dictionary_means
    
    def best5(self):
        dictionary = self.calculate_all_means()
        sorted_dict = dict()
        rez = []
        if self.question in webserver.data_ingestor.questions_best_is_max:
            sorted_dict(sorted(dictionary.items(), key= lambda t: t[1], reverse = True))
            rez = more_itertools.take(5, sorted_dict.items)
        else:
            sorted_dict(sorted(dictionary.items(), key= lambda t: t[1]))
            rez = more_itertools.take(5, sorted_dict.items)
        self.result = jsonify(rez)

    def worst5(self):
        dictionary = self.calculate_all_means()
        sorted_dict = dict()
        rez = []
        if self.question in webserver.data_ingestor.questions_best_is_max:
            sorted_dict(sorted(dictionary.items(), key= lambda t: t[1]))
            rez = more_itertools.take(5, sorted_dict.items)
        else:
            sorted_dict(sorted(dictionary.items(), key= lambda t: t[1], reverse = True))
            rez = more_itertools.take(5, sorted_dict.items)
        self.result = jsonify(rez)

    def global_mean(self):
        sum = 0
        counter = 0
        # Add synch here
        if webserver.data_ingestor.global_mean is not -1:
            self.result = jsonify(webserver.data_ingestor.global_mean)
            return
        # end synch

        for row in webserver.data_ingestor.dictionary_data:
            sum += row["Data_Value"]
            counter +=1
        rez = sum / counter
        self.result = jsonify(rez)
        # add synch here
        webserver.data_ingestor.global_mean = rez
        # end synch

    def state_diff_from_mean(self):
        state_mean = self.calculate_state_mean(self.state)
        self.global_mean()
        # add synch
        global_mean = webserver.data_ingestor.global_mean
        # end synch

        rez = global_mean - state_mean
        self.result(jsonify(rez))
        return rez
    
    def diff_from_mean(self):
        states_means = dict()
        for row in webserver.data_ingestor.dictionary_data:
            if row["State"] not in states_means:
                states_means.update(row["State"], self.state_diff_from_mean(row["State"]))
        
        rez = jsonify(states_means)
        self.result = rez


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
        self._create_workers()
    
    def submit_task(self, task):
        self.tasks_queue.append(task)
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
            self.workers.append(worker)

class TaskRunner(Thread):
    def __init__(self, task_queue):
        # TODO: init necessary data structures
        Thread.__init__(self)
        self.task_queue = task_queue

    def run(self):
        while True:
            # TODO
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            job = self.task_queue.get()
            result = job.execute()
            pass
