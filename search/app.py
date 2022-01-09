import sqlite3
import logging
import pika
import time
import threading
import json
from flask import Flask
from flask import request
from flask import Response
import consul
import os
import requests

app = Flask(__name__)


@app.route("/")
def hello():
    return "Hello World from the search service!"


@app.route("/search")
def search():
    start = request.args.get("date")
    duration = request.args.get("duration")

    if start == None:
        return Response(
            '{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a start for the search."}',
            status=400, mimetype="application/json")

    if duration == None:
        return Response(
            '{"result": false, "error": 1, "description": "Cannot proceed because you did not provide a duration date for the reservation."}',
            status=400, mimetype="application/json")

    duration = int(duration)

    # Connect and setup the database
    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS reservation (id text, name text, start text, duration INTEGER, vip INTEGER)")
    cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, squaremeters INTEGER)")

    # Check for free apartments
    # this SQL query lists all unique names+size which do now have any conflicting reservation
    cursor.execute(f"""SELECT DISTINCT a.name as name, a.squaremeters as size
                      FROM appartments a 
                      WHERE NOT EXISTS (
                        SELECT r.name 
                        FROM reservation r 
                        WHERE 
                            r.name = a.name 
                          AND 
                              (date(r.start) BETWEEN date('{start}') AND date('{start}', '+{duration-1} days') 
                            OR  
                              date(r.start, '+'||r.duration||' days') 
                                BETWEEN 
                                  date('{start}', '+{duration-1} days') 
                                AND 
                                  date('{start}', '+{duration} days') 
                            OR 
                                (date(r.start, '+'||r.duration||' days') >= date('{start}', '+{duration} days') 
                              AND 
                                date(r.start) <= date('{start}') 
                        )));""")
    apps = cursor.fetchall()
    if len(apps) == 0:
        return Response(
            '{"result": false, "error": 2, "description": "No results matched the criteria."}',
            status=400, mimetype="application/json")

    output = "<h4>Name,Size(sq m)</h4>"
    for record in apps:
        output += f"<p>{record[0]},{record[1]}</p>\n"

    connection.close()

    return f'<p>Found {len(apps)} results.</p><p>{output}</p>'


def appartment_added(ch, method, properties, body):
    logging.info("Apartment added message received.")
    data = json.loads(body)
    id = data["id"]
    name = data["name"]
    size = data["size"]

    logging.info(f"Adding appartment {name}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("INSERT INTO appartments VALUES (?, ?, ?)", (id, name, size))
    cursor.close()
    connection.close()


def apartment_deleted(ch, method, properties, body):
    logging.info("Apartment deleted message received.")
    data = json.loads(body)
    id = data["id"]
    name = data["name"]

    logging.info(f"Deleting appartment {name}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM appartments WHERE id = ?", (id,))
    cursor.close()
    connection.close()


def reservation_added(ch, method, properties, body):
    logging.info("Reservation added message received.")
    data = json.loads(body)
    id = data["id"]
    name = data["name"]
    start = data["start"]
    duration = data["duration"]
    vip = data["vip"]

    logging.info(f"Adding reservation {id}({name})...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("INSERT INTO reservation VALUES (?, ?, ?, ?, ?)", (id, name, start, duration, vip))
    cursor.close()
    connection.close()


def reservation_deleted(ch, method, properties, body):
    logging.info("Reservation deleted message received.")
    data = json.loads(body)
    id = data["id"]

    logging.info(f"Deleting reservation {id}...")

    connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM reservation WHERE id = ?", (id,))
    cursor.close()
    connection.close()


def connect_to_mq():
    while True:
        time.sleep(10)

        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        except Exception as e:
            logging.warning(f"Could not start listening to the message queue, retrying...")


def listen_to_events(channel):
    channel.start_consuming()


def register():
    while True:
        try:
            connection = consul.Consul(host='consul', port=8500)
            connection.agent.service.register("search", address="127.0.0.1", port=5003)
            break
        except (ConnectionError, consul.ConsulException):
            logging.warning('Consul is down, reconnecting...')
            time.sleep(5)


def deregister():
    connection = consul.Consul(host='consul', port=8500)
    connection.agent.service.deregister("search", address="search", port=5002)


def find_service(name):
    connection = consul.Consul(host="consul", port=8500)
    _, services = connection.health.service(name, passing=True)
    for service_info in services:
        address = service_info["Service"]["Address"]
        port = service_info["Service"]["Port"]
        return address, port

    return None, None


if __name__ == "__main__":
    logging.basicConfig(format="%(message)s", level=1 * 10)
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("sqlite3").setLevel(logging.WARNING)

    logging.info("Start.")

    register()

    connection = connect_to_mq()

    channel = connection.channel()
    channel.exchange_declare(exchange="appartments", exchange_type="direct")

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="appartments", queue=queue_name, routing_key="added")
    channel.basic_consume(queue=queue_name, on_message_callback=appartment_added, auto_ack=True)

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="appartments", queue=queue_name, routing_key="deleted")
    channel.basic_consume(queue=queue_name, on_message_callback=apartment_deleted, auto_ack=True)

    channel.exchange_declare(exchange="reservations", exchange_type="direct")

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="reservations", queue=queue_name, routing_key="added")
    channel.basic_consume(queue=queue_name, on_message_callback=reservation_added, auto_ack=True)

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="reservations", queue=queue_name, routing_key="deleted")
    channel.basic_consume(queue=queue_name, on_message_callback=reservation_deleted, auto_ack=True)

    logging.info("Waiting for messages.")

    thread = threading.Thread(target=listen_to_events, args=(channel,), daemon=True)
    thread.start()

    # Verify if database has to be initialized
    database_is_initialized = False
    if os.path.exists("/home/data/search.db"):
        database_is_initialized = True
    else:
        connection = sqlite3.connect("/home/data/search.db", isolation_level=None)
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS appartments (id text, name text, squaremeters INTEGER)")

        address, port = find_service("appartments")
        if address is not None and port is not None:
            response = requests.get(f"http://{address}:{port}/appartments")
            data = response.json()

            logging.info("Received data: " + data)

            for entry in data["appartments"]:
                cursor.execute("INSERT INTO appartments VALUES (?, ?, ?)", (entry["id"], entry["name"], entry["squaremeters"]))

            database_is_initialized = True

        cursor.execute(
            "CREATE TABLE IF NOT EXISTS reservation (id text, name text, start text, duration INTEGER, vip INTEGER)")

        address, port = find_service("reserve")
        if address is not None and port is not None:
            response = requests.get(f"http://{address}:{port}/reservations")
            data = response.json()

            logging.info("Received data: " + data)

            for entry in data["reservation"]:
                cursor.execute("INSERT INTO reservation VALUES (?, ?, ?, ?, ?)",
                               (entry["id"], entry["name"], entry["start"], entry["duration"], entry["vip"]))

            database_is_initialized = True

    if not database_is_initialized:
        logging.error("Cannot initialize database.")
    else:
        logging.info("Starting the web server.")

        try:
            app.run(host="0.0.0.0", threaded=True)
        finally:
            connection.close()
            deregister()
