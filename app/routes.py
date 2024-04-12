from app import webserver
from flask import request, jsonify
from app.task_runner import Job

import os
import json
import logging

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

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
    print(f"JobID is {job_id}")
    # TODO
    # Check if job_id is valid
    # do some synch
    for task in webserver.tasks_runner.all_tasks :
        if task.id == job_id :
            if task.status == "Done":
                print(f"found a guy done " + task.status + " " + task.type)
                return jsonify({
                    'status': 'done',
                    'data': task.result
                })
            elif task.status == 'Running':
                print(f"he runnnin' boys " + task.status)
                return jsonify({'status': 'running'})
    
    # Check if job_id is done and return the result
    #    res = res_for(job_id)
    #    return jsonify({
    #        'status': 'done',
    #        'data': res
    #    })

    # If not, return running status
    print(f"nothing was found")
    return jsonify({'status': "error",
                    'reason': 'Invalid job_id'})

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Get request data
    data = request.json
    print(f"Got request {data}")
    webserver.tasks_runner.logger.info("States_mean with this data: %s", data)
    # TODO
    # Register job. Don't wait for task to finish
    job_id = webserver.job_counter
    job_id_string = "job_id_" + str(job_id)
    new_job = Job(job_id_string, "states_mean", webserver.data_ingestor)
    new_job.set_question(data['question'])
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)
    # Increment job_id counter
    # Return associated job_id
    webserver.tasks_runner.logger.info("States_mean job %s", str(job_id))
    return jsonify({"job_id": "job_id_" + str(job_id) })

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    # TODO
    # Get request data
    data = request.json
    print(f"Got request {data}")

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    job_id = webserver.job_counter
    job_id_string = "job_id_" + str(job_id)
    new_job = Job(job_id_string, "state_mean",webserver.data_ingestor)
    new_job.set_question(data['question'])
    new_job.set_state(data['state'])
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + str(job_id) })


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # TODO
    # Get request data
    data = request.json
    print(f"Got request {data}")
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated 
    job_id = webserver.job_counter
    job_id_string = "job_id_" + str(job_id)
    new_job = Job(job_id_string, "best5",webserver.data_ingestor)
    new_job.set_question(data['question'])
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + str(job_id) })

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    # TODO
    # Get request data
    data = request.json
    print(f"Got request {data}")
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    job_id = webserver.job_counter
    job_id_string = "job_id_" + str(job_id)
    new_job = Job(job_id_string, "worst5",webserver.data_ingestor)
    new_job.set_question(data['question'])
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + str(job_id) })

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    # TODO
    # Get request data
    data = request.json
    print(f"Got request {data}")
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    job_id = webserver.job_counter
    job_id_string = "job_id_" + str(job_id)
    new_job = Job(job_id_string, "global_mean",webserver.data_ingestor)
    new_job.set_question(data['question'])
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + str(job_id) })

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    # TODO
    # Get request data
    data = request.json
    print(f"Got request {data}")
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    job_id = webserver.job_counter
    job_id_string = "job_id_" + str(job_id)
    new_job = Job(job_id_string, "diff_from_mean",webserver.data_ingestor)
    new_job.set_question(data['question'])
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)
    return jsonify({"job_id": "job_id_" + str(job_id) })

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    # TODO
    # Get request data
    data = request.json
    print(f"Got request {data}")
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    job_id = webserver.job_counter
    job_id_string = "job_id_" + str(job_id)
    new_job = Job(job_id_string, "state_diff_from_mean",webserver.data_ingestor)
    new_job.set_question(data['question'])
    new_job.set_state(data['state'])
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + str(job_id) })

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # TODO
    # Get request data
    data = request.json
    print(f"Got request {data}")
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    job_id = webserver.job_counter
    job_id_string = "job_id_" + str(job_id)
    new_job = Job(job_id_string, "mean_by_category",webserver.data_ingestor)
    new_job.set_question(data['question'])
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + str(job_id) })

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # TODO
    # Get request data
    data = request.json
    print(f"Got request {data}")
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id
    job_id = webserver.job_counter
    job_id_string = "job_id_" + str(job_id)
    new_job = Job(job_id_string, "state_mean_by_category",webserver.data_ingestor)
    new_job.set_question(data['question'])
    new_job.set_state(data['state'])
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + str(job_id) })

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    webserver.tasks_runner.active = False
    webserver.tasks_runner.wait_completion()
    return jsonify({'status' : 'closing down'})

@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    return jsonify({"data": webserver.tasks_runner.tasks_queue.qsize()})

@webserver.route('/api/jobs', methods=['GET'])
def jobs_list():
    job_dict = dict()
    for job in webserver.tasks_runner.all_tasks:
        job_dict[job.id] = job.status
    
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
