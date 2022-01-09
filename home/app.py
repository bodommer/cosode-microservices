from flask import request
from flask import Flask, redirect
from flask import Response
import logging
import consul

app = Flask(__name__)


@app.route("/appartments/add")
def add_app():
    service = findService("appartments")
    print(f"service details: {service[0]}, {service[1]}")
    if service[0] is not None and service[1]:
        return redirect(f"http://{service[0]}:{service[1]}/add?{request.query_string.decode('utf-8')}", code=302)
    return Response(
        '{"result": false, "error": 3, "description": "Cannot process the apartment add request because the service could not be found."}',
        status=404, mimetype="application/json")


@app.route("/appartments/remove")
def remove_app():
    service = findService("appartments")
    print(f"service details: {service[0]}, {service[1]}")
    if service[0] is not None and service[1]:
        return redirect(f"http://{service[0]}:{service[1]}/remove?{request.query_string.decode('utf-8')}", code=302)
    return Response(
        '{"result": false, "error": 3, "description": "Cannot process the apartment remove request because the service could not be found."}',
        status=404, mimetype="application/json")


@app.route("/reserve/add")
def add_res():
    service = findService("reserve")
    print(f"service details: {service[0]}, {service[1]}")
    if service[0] is not None and service[1]:
        return redirect(f"http://{service[0]}:{service[1]}/add?{request.query_string.decode('utf-8')}", code=302)
    return Response(
        '{"result": false, "error": 3, "description": "Cannot process the reservation add request because the service could not be found."}',
        status=404, mimetype="application/json")


@app.route("/reserve/remove")
def remove_res():
    service = findService("reserve")
    print(f"service details: {service[0]}, {service[1]}")
    if service[0] is not None and service[1]:
        return redirect(f"http://{service[0]}:{service[1]}/remove?{request.query_string.decode('utf-8')}", code=302)
    return Response(
        '{"result": false, "error": 3, "description": "Cannot process the reservation remove request because the service could not be found."}',
        status=404, mimetype="application/json")


@app.route("/search")
def add():
    service = findService("search")
    print(f"service details: {service[0]}, {service[1]}")
    if service[0] is not None and service[1]:
        return redirect(f"http://{service[0]}:{service[1]}/search?{request.query_string.decode('utf-8')}", code=302)
    return Response(
        '{"result": false, "error": 3, "description": "Cannot process the search request because the service could not be found."}',
        status=404, mimetype="application/json")


@app.route("/")
def hello():
    return "Hello World from home!"


def findService(name):
    connection = consul.Consul(host="consul", port=8500)
    _, services = connection.health.service(name, passing=True)
    for service_info in services:
        address = service_info["Service"]["Address"]
        port = service_info["Service"]["Port"]
        return address, port

    return None, None


if __name__ == "__main__":
    logging.info("Starting the web server.")

    app.run(host="0.0.0.0", threaded=True)
