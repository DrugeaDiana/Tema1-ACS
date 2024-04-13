from queue import Queue
from threading import Thread
import os
import time
import multiprocessing
import json
import logging
from logging.handlers import RotatingFileHandler

class Job:
    '''Implements the object that deals with the operation
    made on the set of data'''
    def __init__(self, id, type, data_ingestor):
        self.id = id
        self.type = type
        self.result = None
        self.state = None
        self.status = 'pending'
        self.data_ingestor = data_ingestor
        self.question = None

    def set_question(self, question):
        '''Sets the question the job has to calculate data about'''
        self.question = question

    def set_state(self, state):
        '''Sets the state for which the object has to calculate things for'''
        self.state = state

    def execute(self):
        '''Checks what it has to calculate and calls the specific function'''
        self.status = 'running'
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

        # Once it's done it returns from the function it just called
        self.status = 'done'

    def calculate_all_means(self):
        ''' Calculate all the means for each state that we have data
        which answers the given question'''
        cols = ["Question", "LocationDesc"]
        col = 'Question'
        rez = self.data_ingestor.data.loc[self.data_ingestor.data[col]
                       == self.question].groupby(cols)['Data_Value'].mean()
        return rez

    def turn_result_in_dict(self, rez, value = -1):
        '''Turns the series object resulted from processesing the
        dataframe'''
        rez = rez.to_dict()
        values_dict = {}
        if value == -1:
            for key in rez.keys():
                values_dict[key[1]] = rez[key]
        else:
            for key in rez.keys():
                values_dict[key[1]] =  value - rez[key]
        return values_dict

    def states_mean(self):
        '''Calculates all means and sorts them in ascending order
        Saves the dictionary resulted in the job'''
        rez = self.calculate_all_means()
        rez = rez.sort_values()
        self.result = self.turn_result_in_dict(rez)

    def calculate_state_mean(self, state):
        '''Calculates the mean for the given state'''
        cols = ["Question", "LocationDesc"]
        rez = self.data_ingestor.data.groupby(cols)
        rez = rez.get_group((self.question, state))['Data_Value'].mean()
        return rez

    def state_mean(self):
        '''Function called for the "state_mean" route
        Saves the resulted data into a dictionary with one entry'''
        rez = self.calculate_state_mean(self.state)
        self.result = {self.state : rez}

    def top_order(self, rez):
        '''Gets the first 5 elements in the resulted series and puts
        them in a dictionary'''
        sorted_dict = {}
        rez = rez.to_dict()
        counter = 0
        for key in rez.keys():
            sorted_dict[key[1]] = rez[key]
            counter += 1
            if counter == 5:
                break
        return sorted_dict

    def best5(self):
        '''Calculates the states with the best means for the given
        question'''
        rez = self.calculate_all_means()
        if self.question in self.data_ingestor.questions_best_is_max:
            rez = rez.sort_values(ascending=False)
        else:
            rez = rez.sort_values()
        self.result = self.top_order(rez)

    def worst5(self):
        '''Calculates the states with the worst means for the given
        question'''
        rez = self.calculate_all_means()
        if self.question in self.data_ingestor.questions_best_is_max:
            rez = rez.sort_values()
        else:
            rez = rez.sort_values(ascending = False)
        self.result = self.top_order(rez)

    def global_mean(self):
        '''Calculates the global_mean for the given question'''
        rez = self.data_ingestor.data.groupby(['Question'])
        rez = rez.get_group((self.question,))['Data_Value'].mean()

        # Puts the result in a dictionary for when the global_mean route
        # is calculated
        rez_dict = {}
        rez_dict["global_mean"] = rez
        self.result = rez_dict

        # Returns the resulted value of global_mean to use it in other functions
        return rez

    def state_diff_from_mean(self):
        '''Calculates the difference between global_mean and the mean
        for a given state'''
        state_mean = self.calculate_state_mean(self.state)
        global_mean = self.global_mean()
        rez = global_mean - state_mean
        self.result = {self.state : rez }

    def diff_from_mean(self):
        ''' Calculates the difference between global_mean and the mean
        of all the states that answer the given question'''
        global_mean = self.global_mean()
        rez = self.calculate_all_means()
        self.result = self.turn_result_in_dict(rez, global_mean)

    def mean_by_category(self):
        '''Calculates the mean for each state and each category in that state
        that answers a given question'''
        cols = ['Question', 'LocationDesc', 'StratificationCategory1', 'Stratification1']
        rez = self.data_ingestor.data.loc[self.data_ingestor.data['Question']
                                               == self.question].groupby(cols)['Data_Value'].mean()

        # Puts the data in a dictionary with a string key instead of a tuple
        rez.to_dict()
        dictionary_result = {}
        for key in rez.keys():
            key_string = "('" + key[1] + "', '" + key[2] + "', '" + key[3] + "')"
            dictionary_result[key_string] = rez[key]
        self.result = dictionary_result

    def state_mean_by_category(self):
        '''Calculates the mean for the given stateand each category that answer the
        question'''
        cols = ['Question', 'LocationDesc', 'StratificationCategory1', 'Stratification1']
        rez = self.data_ingestor.data[(self.data_ingestor.data['Question']
                                            == self.question) &
                                            (self.data_ingestor.data['LocationDesc'] == self.state)]
        rez = rez.groupby(cols)['Data_Value'].mean()

        # Turns the series resulted into a dictionary with string key
        rez.to_dict()
        dictionary_result = {}
        for key in rez.keys():
            key_string = "('" + key[2] + "', '" + key[3] + "')"
            dictionary_result[key_string] = rez[key]

        # Formats the data so it fits the output given :)
        json_result = {}
        json_result[self.state] = dictionary_result
        self.result = json_result

class ThreadPool:
    '''Class that organizes the threads'''

    def __init__(self):
        '''Initializes the ThreadPool and the objects it uses'''
        self.create_directories()
        self.num_threads = self.get_num_threads()
        self.tasks_queue = Queue()
        self.all_tasks = []
        self.threads = []
        self.create_threads()
        self.active = True
        self.logger = self.logger_setup()

    def create_directories(self):
        '''Creates the directories used for the program'''
        # Stores results
        if not os.path.exists("results"):
            os.mkdir("results")

        # Stores the log files
        if not os.path.exists("logs"):
            os.mkdir("logs")

    def logger_setup(self):
        '''Initializes the logger used by the server'''
        logger = logging.getLogger(__name__)
        string_format = '[%(asctime)s] %(funcName)20s(): %(message)s'
        logging.basicConfig(
            handlers = [RotatingFileHandler('logs/webserver.log', maxBytes=7000, backupCount=100)],
            level=logging.DEBUG,
            format = string_format)
        logging.Formatter.converter = time.gmtime
        return logger

    def submit_task(self, task):
        '''Submits task to the threadpool and saves the job in the
        list with everything executed so far'''
        if self.active is True:
            self.tasks_queue.put(task)
            self.all_tasks.append(task)

    def get_num_threads(self):
        '''Initializes the number of threads we can use'''
        env_var = os.getenv('TP_NUM_OF_THREADS')
        if env_var is not None:
            return int(env_var)
        return multiprocessing.cpu_count()

    def create_threads(self):
        '''Creates all the threads that we use'''
        for _ in range(self.num_threads):
            new_thread = TaskRunner(self.tasks_queue)
            new_thread.start()
            self.threads.append(new_thread)

    def wait_completion(self):
        '''Makes sure that no new job is queued anymore and tells
        the threads to shutdown'''
        self.tasks_queue.join()
        self.active = False
        for _ in range(self.num_threads):
            self.submit_task(None)

class TaskRunner(Thread):
    def __init__(self, task_queue):
        '''Initializes the thread and gives it the queue of tasks it can get'''
        Thread.__init__(self)
        self.task_queue = task_queue

    def run(self):
        '''The function that the thread loops trough'''
        while True:
            # Get first available job from the queue of pending
            # jobs
            job = self.task_queue.get()

            # Checks to see if the thread has to stop
            if job is None:
                self.task_queue.task_done()
                break

            # Execute the job
            job.execute()
            file_string = "results/" + job.id
            with open(file_string, "w") as file:
                json.dump(job.result, file)

            # Announce that the queue can be accessed by other threads
            self.task_queue.task_done()
