from queue import Queue
from threading import Thread, Event
import os
import time
import multiprocessing

class Job:
    def __init__(self, id, type):
        self.id = id
        self.type = type

    def execute(self):
        match self.type:
            case 'states_mean':
                return 1
            case 'state_mean':
                return 1
            case 'best5':
                return 1
            case 'worst5':
                return 1
            case 'global_mean':
                return 1
            case 'diff_from_mean':
                return 1
            case 'state_diff_from_mean':
                return 1
            case 'mean_by_category':
                return 1
            case 'state_mean_by_category':
                return 1
            case _:
                return 0

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
