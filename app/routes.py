from app import webserver
from flask import request, jsonify
from app.task_runner import Job

import os
import json

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

    for task in webserver.task_runner.all_tasks :
        if task.id == job_id :
            if task.status == "Done":
                return jsonify({
                    'status': 'done',
                    'data': res
                })
            else:
                return jsonify({'status': 'running'})
    
    # Check if job_id is done and return the result
    #    res = res_for(job_id)
    #    return jsonify({
    #        'status': 'done',
    #        'data': res
    #    })

    # If not, return running status
    return jsonify({'status': "error",
                    'reason': 'Invalid job_id'})

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Get request data
    data = request.json
    print(f"Got request {data}")

    # TODO
    # Register job. Don't wait for task to finish
    job_id = webserver.job_counter
    new_job = Job(job_id, "states_mean")
    new_job.set_question(data.question)
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"job_id": "job_id_" + job_id })

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
    new_job = Job(job_id, "state_mean")
    new_job.set_question(data.question)
    new_job.set_state(data.state)
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + job_id })


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
    new_job = Job(job_id, "best5")
    new_job.set_question(data.question)
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + job_id })

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
    new_job = Job(job_id, "worst5")
    new_job.set_question(data.question)
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + job_id })

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
    new_job = Job(job_id, "global_mean")
    new_job.set_question(data.question)
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + job_id })

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
    new_job = Job(job_id, "diff_from_mean")
    new_job.set_question(data.question)
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)
    return jsonify({"job_id": "job_id_" + job_id })

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
    new_job = Job(job_id, "state_diff_from_mean")
    new_job.set_question(data.question)
    new_job.set_state(data.state)
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + job_id })

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
    new_job = Job(job_id, "state_diff_from_mean")
    new_job.set_question(data.question)
    new_job.set_state(data.state)
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + job_id })

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
    new_job = Job(job_id, "state_mean_by_category")
    new_job.set_question(data.question)
    new_job.set_state(data.state)
    webserver.job_counter += 1
    webserver.tasks_runner.submit_task(new_job)

    return jsonify({"job_id": "job_id_" + job_id })

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
