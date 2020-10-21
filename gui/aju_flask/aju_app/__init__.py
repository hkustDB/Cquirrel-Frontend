from flask import Flask
from flask_bootstrap import Bootstrap
from flask_socketio import SocketIO
import time
from threading import Lock
import psutil
from confluent_kafka import Consumer
import os
import time

from config import config_options
from config import OUTPUT_DATA_FILE

bootstrap = Bootstrap()
socketio = SocketIO()
thread = None
thread_lock = Lock()


def create_app(config_name):

    if os.path.exists(OUTPUT_DATA_FILE):
        os.truncate(OUTPUT_DATA_FILE, 0)
        print('truncate OUTPUT_DATA_FILE.')
    else:
        f = open(OUTPUT_DATA_FILE, 'w')
        f.close()

    app = Flask(__name__)
    app.config.from_object(config_options[config_name])
    config_options[config_name].init_app(app)

    bootstrap.init_app(app)
    socketio.init_app(app)

    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    return app


@socketio.on('connect')
def send_kafka_data():
    global thread
    with thread_lock:
        if thread is None:
            thread = socketio.start_background_task(target=background_send_kafka_data_thread)


def background_send_kafka_data_thread():
    with open(OUTPUT_DATA_FILE, 'r') as f:
        while True:
            time.sleep(0.1)
            line = f.readline()
            if line:
                line_list = line.strip().lstrip('(').rstrip(')').split(',')
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "  send: " + str(line_list))
                socketio.emit('result_figure_data', {'data': line_list})
            else:
                f.seek(0)


def background_send_kafka_data_thread_real():
    KAFKA_HOME_PATH = '/Users/chaoqi/Programs/kafka_2.12-2.6.0'
    KAFKA_CONSUMER_BOOTSTRAP_SERVERS = 'localhost:9092'
    KAFKA_CONSUMER_GROUP_ID = 'aju_generated_jar_output_group'
    KAFKA_CONSUMER_TOPIC = 'aju_generated_jar_output'
    KAFKA_CONSUMER_AUTO_OFFSET_RESET = 'earliest'

    kafka_consumer = Consumer({
        'bootstrap.servers': KAFKA_CONSUMER_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_CONSUMER_GROUP_ID,
        'auto.offset.reset': KAFKA_CONSUMER_AUTO_OFFSET_RESET
    })
    kafka_consumer.subscribe([KAFKA_CONSUMER_TOPIC])

    while True:
        time.sleep(1)
        msg = kafka_consumer.poll(1)
        if msg:
            print(msg)
            msg_list = msg.value().decode('utf-8').replace('\n', '').replace('\r', '').split(',')
            print("send: ", str(msg_list))
            socketio.emit('result_figure_data', {'data': msg_list})
