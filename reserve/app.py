import sqlite3
import uuid
from flask import request
from flask import Flask
from flask import Response
import logging
import pika
import json
import consul
import time

app = Flask(__name__)


@app.route("/add")
def add():
    id = uuid.uuid4()
    name = request.args.get("name")
    start = request.args.get("start")
    duration = request.args.get("duration")
    vip = request.args.get("vip")

    if name == None:
        return Response(
            '{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a name for the reservation."}',
            status=400, mimetype="application/json")

    if start == None:
        return Response(
            '{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a start date for the reservation."}',
            status=400, mimetype="application/json")

    if duration == None:
        return Response(
            '{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a duration of the stay for the reservation."}',
            status=400, mimetype="application/json")

    if vip == None:
        return Response(
            '{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a the VIP value for the reservation."}',
            status=400, mimetype="application/json")

    # Connect and setup the database
    connection = sqlite3.connect("/home/data/reservations.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS reservation (id text, name text, start text, duration INTEGER, vip INTEGER)")

    # Check if reservation already exists
    cursor.execute("SELECT COUNT(id) FROM reservation WHERE name = ? AND start = ? AND duration = ?",
                   (name, start, duration))
    already_exists = cursor.fetchone()[0]
    if already_exists > 0:
        return Response(
            '{"result": false, "error": 2, "description": "Cannot proceed because this reservation already exists"}',
            status=400, mimetype="application/json")

    # Add reservation
    cursor.execute("INSERT INTO reservation VALUES (?, ?, ?, ?, ?)", (str(id), name, start, str(duration), str(vip)))
    cursor.close()
    connection.close()

    # Notify everybody that the reservation was added
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="reservations", exchange_type="direct")
    channel.basic_publish(exchange="reservations", routing_key="added",
                          body=json.dumps(
                              {"id": str(id), "name": name, "start": start, "duration": str(duration), "vip": vip}))
    connection.close()

    return Response('{"result": true, "description": "Reservation was added successfully."}', status=201,
                    mimetype="application/json")


@app.route("/")
def hello():
    return "Hello World from reservations!"


@app.route("/reservations")
def get_all():
    print("RES info requested")

    connection = sqlite3.connect("/home/data/reservations.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS reservation (id text, name text, start text, duration INTEGER, vip INTEGER)")

    # get data
    cursor.execute("SELECT * FROM reservation")
    result = cursor.fetchall()

    # close connection
    cursor.close()
    connection.close()

    return json.dumps([dict(ix) for ix in result])


@app.route("/remove")
def delete():
    id = request.args.get("id")

    if id == None:
        return Response(
            '{"result": false, "error": 1, "description": "Cannot proceed because you did not provide an id for the reservation to delete."}',
            status=400, mimetype="application/json")

    # Connect and setup the database
    connection = sqlite3.connect("/home/data/reservations.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS reservation (id text, name text, start text, duration INTEGER, vip INTEGER)")

    # Check if reservation exists
    cursor.execute("SELECT COUNT(id) FROM reservation WHERE id = ?", (str(id),))
    res_id = cursor.fetchone()[0]
    if res_id is None:
        return Response(
            '{"result": false, "error": 2, "description": "Cannot proceed because this reservation does not exist"}',
            status=400, mimetype="application/json")

    # remove reservation
    cursor.execute("DELETE FROM reservation WHERE id = ?", (str(res_id),))
    cursor.close()
    connection.close()

    # Notify everybody that the reservation was deleted
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    channel.exchange_declare(exchange="reservations", exchange_type="direct")
    channel.basic_publish(exchange="reservations", routing_key="deleted",
                          body=json.dumps({"id": str(id)}))
    connection.close()

    return Response('{"result": true, "description": "Reservation was deleted successfully."}', status=201,
                    mimetype="application/json")


def register():
    while True:
        try:
            connection = consul.Consul(host='consul', port=8500)
            connection.agent.service.register("reserve", address="127.0.0.1", port=5002)
            break
        except (ConnectionError, consul.ConsulException):
            logging.warning('Consul is down, reconnecting...')
            time.sleep(5)


if __name__ == "__main__":
    logging.info("Starting the web server.")

    register()

    app.run(host="0.0.0.0", threaded=True)
