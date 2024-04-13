from flask import request, jsonify
from app import webserver
from app.task_runner import Job

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    '''Get results from a job if they're available'''
    logger_text = "Input: " + str(job_id)
    webserver.tasks_runner.logger.info(logger_text)

    # Searching for the specific task
    for task in webserver.tasks_runner.all_tasks :
        if task.id == job_id :
            # If found, put in log that we found it with the specific status
            logger_text = "Output: status: " + task.status
            webserver.tasks_runner.logger.info(logger_text)

            if task.status == "done":
                logger_text = "Job is done. Result: " + str(task.result)
                return jsonify({
                    'status': 'done',
                    'data': task.result
                })

            if task.status == 'running':
                return jsonify({'status': 'running'})

    # Job_id not found logged
    logger_text = "Get_results output: Invalid job_id"
    webserver.tasks_runner.logger.error(logger_text)

    # If not, return running status
    return jsonify({'status': "error",
                    'reason': 'Invalid job_id'})

def create_job(task_name, data, state = False) :
    '''Creates a new job and submits it into the threadpool's queue'''
    job_id = webserver.job_counter
    job_id_string = "job_id_" + str(job_id)
    new_job = Job(job_id_string, task_name, webserver.data_ingestor)
    new_job.set_question(data['question'])
    if state is True:
        new_job.set_state(data['state'])
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)
    return job_id_string

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    '''Handles the states_mean_request'''
    # Get request data
    data = request.json

    # Input Log
    logger_text = "Input: " + str(data)
    webserver.tasks_runner.logger.info(logger_text)

    # Build new job and submit it
    job_id_string = create_job("states_mean", data, False)

    # Log for the return value of the request:
    logger_text = "Return: " + job_id_string
    webserver.tasks_runner.logger.info(logger_text)

    return jsonify({"job_id": job_id_string })

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    '''Handles the state_mean_request'''
    # Get request data
    data = request.json

    # Input Log
    logger_text = "Input: " + str(data)
    webserver.tasks_runner.logger.info(logger_text)

    # Build new job and submit it
    job_id_string = create_job("state_mean", data, True)

    # Log for the return value of the request:
    logger_text = "Return: " + job_id_string
    webserver.tasks_runner.logger.info(logger_text)

    return jsonify({"job_id": job_id_string})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    '''Handles best5 request'''
    # Get request data
    data = request.json

    # Input Log
    logger_text = "Input: " + str(data)
    webserver.tasks_runner.logger.info(logger_text)

    # Build new job and submit it
    job_id_string = create_job("best5", data, False)

    # Log for the return value of the request:
    logger_text = "Return: " + job_id_string
    webserver.tasks_runner.logger.info(logger_text)

    return jsonify({"job_id": job_id_string })

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    '''Handles worst5 request'''
    # Get request data
    data = request.json

    # Input Log
    logger_text = "Input: " + str(data)
    webserver.tasks_runner.logger.info(logger_text)

    # Build new job and submit it
    job_id_string = create_job("worst5", data, False)

    # Log for the return value of the request:
    logger_text = "Return: " + job_id_string
    webserver.tasks_runner.logger.info(logger_text)

    return jsonify({"job_id": job_id_string })

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    '''Handles global_mean request'''
    data = request.json

    # Input Log
    logger_text = "Input: " + str(data)
    webserver.tasks_runner.logger.info(logger_text)

    # Build new job and submit it
    job_id_string = create_job("global_mean", data, False)

    # Log for the return value of the request:
    logger_text = "Return: " + job_id_string
    webserver.tasks_runner.logger.info(logger_text)

    return jsonify({"job_id": job_id_string })

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    '''Handles diff_from_mean request'''
    # Get request data
    data = request.json

    # Input Log
    logger_text = "Input: " + str(data)
    webserver.tasks_runner.logger.info(logger_text)

    # Build new job and submit it
    job_id_string = create_job("diff_from_mean", data, False)

    # Log for the return value of the request:
    logger_text = "Return: " + job_id_string
    webserver.tasks_runner.logger.info(logger_text)

    return jsonify({"job_id": job_id_string })

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    '''Handles state_diff_from_mean request'''
    # Get request data
    data = request.json

    # Input Log
    logger_text = "Input: " + str(data)
    webserver.tasks_runner.logger.info(logger_text)

    # Build new job and submit it
    job_id_string = create_job("state_diff_from_mean", data, True)

    # Log for the return value of the request:
    logger_text = "Return: " + job_id_string
    webserver.tasks_runner.logger.info(logger_text)

    return jsonify({"job_id": job_id_string })

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    '''Handles mean_by_category request'''
    # Get request data
    data = request.json

    # Input Log
    logger_text = "Input: " + str(data)
    webserver.tasks_runner.logger.info(logger_text)

    # Build new job and submit it
    job_id_string = create_job("mean_by_category", data, False)

    # Log for the return value of the request:
    logger_text = "Return: " + job_id_string
    webserver.tasks_runner.logger.info(logger_text)

    return jsonify({"job_id": job_id_string })

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    '''Handles state_mean_by_category request'''
    # Get request data
    data = request.json

    # Input Log
    logger_text = "Input: " + str(data)
    webserver.tasks_runner.logger.info(logger_text)

    # Build new job and submit it
    job_id_string = create_job("state_mean_by_category", data, True)

    # Log for the return value of the request:
    logger_text = "Return: " + job_id_string
    webserver.tasks_runner.logger.info(logger_text)

    return jsonify({"job_id": job_id_string })

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    '''Handles graceful_shutdown request'''
    webserver.tasks_runner.active = False
    webserver.tasks_runner.wait_completion()

    # Log for the return value of the request:
    logger_text = "Server is closing down"
    webserver.tasks_runner.logger.info(logger_text)
    return jsonify({'status' : 'closing down'})

@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    '''Handles num_job request'''
    webserver.tasks_runner.logger.info("Jobs: " + str(webserver.tasks_runner.tasks_queue.qsize()))
    return jsonify({"data": webserver.tasks_runner.tasks_queue.qsize()})

@webserver.route('/api/jobs', methods=['GET'])
def jobs_list():
    '''Returns the list of jobs that have been submited so far'''
    job_dict = {}
    for job in webserver.tasks_runner.all_tasks:
        job_dict[job.id] = job.status

    # Log for the return value of the request:
    logger_text = "Return: " + str(job_dict)
    webserver.tasks_runner.logger.info(logger_text)

    return jsonify({'status' : 'done',
                    'data' : job_dict})

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
