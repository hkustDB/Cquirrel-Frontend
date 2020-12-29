from flask import Flask
from flask_bootstrap import Bootstrap
from flask_socketio import SocketIO
from confluent_kafka import Consumer
import os
import logging
import threading

from config import config_options
from config import Q6_OUTPUT_DATA_FILE
from config import Q3_OUTPUT_DATA_FILE

bootstrap = Bootstrap()
socketio = SocketIO()
# socketio = SocketIO(cors_allowed_origins="*", cors_credentials=False, async_mode='eventlet')
stop_thread_flag = False


def create_app(config_name):
    if os.path.exists(Q6_OUTPUT_DATA_FILE):
        os.truncate(Q6_OUTPUT_DATA_FILE, 0)
        logging.info('truncate the output data file : ' + Q6_OUTPUT_DATA_FILE)
    else:
        f = open(Q6_OUTPUT_DATA_FILE, 'w')
        f.close()

    if os.path.exists(Q3_OUTPUT_DATA_FILE):
        os.truncate(Q3_OUTPUT_DATA_FILE, 0)
        logging.info('truncate the output data file : ' + Q3_OUTPUT_DATA_FILE)
    else:
        f = open(Q3_OUTPUT_DATA_FILE, 'w')
        f.close()

    # logging.getLogger('socketio').setLevel(logging.ERROR)

    app = Flask(__name__)
    app.secret_key = os.urandom(24)
    app.config.from_object(config_options[config_name])
    config_options[config_name].init_app(app)

    bootstrap.init_app(app)
    socketio.init_app(app)

    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    return app


@socketio.on('connect')
def socketio_connect():
    logging.info("socketio connected")


@socketio.on('disconnect')
def socketio_disconnect():
    print('socketio disconnected')


def send_codegen_log_data_to_client(codegen_log_result):
    pass
    # print("send_codegen_log_data_to_client:" + codegen_log_result)
    # while True:
    #     socketio.sleep(2)
    #     print("emit codegen_log")
    #     socketio.emit('codegen_log', {'data': str(codegen_log_result)}, namespace='/ws')


def send_query_result_data_to_client(query_idx):
    global stop_thread_flag
    stop_thread_flag = False
    if query_idx == 3:
        logging.info("sending query " + str(query_idx) + " result data to client...")
        t = threading.Thread(target=send_query_result_data_file, args=(Q3_OUTPUT_DATA_FILE,))
        t.start()
    elif query_idx == 6:
        logging.info("sending query " + str(query_idx) + " result data to client...")
        t = threading.Thread(target=send_query_result_data_file, args=(Q6_OUTPUT_DATA_FILE,))
        t.start()
    else:
        logging.error("query " + str(query_idx) + " does not support for now.")


def send_query_result_data_file(filepath):
    SERVER_SEND_DATA_TO_CLIENT_INTEVAL = 0.5
    socketio.emit('start_figure_data_transmit', {'data': 1})
    with open(filepath, 'r') as f:
        while True:
            global stop_thread_flag
            if stop_thread_flag:
                break

            socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
            line = f.readline()
            if line:
                line_list = line.strip().lstrip('(').rstrip(')').split(',')
                logging.info("send: " + str(line_list))
                socketio.emit('result_figure_data', {'data': line_list})
            else:
                break


def stop_send_data_thread():
    global stop_thread_flag
    stop_thread_flag = True


def background_send_kafka_data_thread(query_idx):
    SERVER_SEND_DATA_TO_CLIENT_INTEVAL = 0.1
    if query_idx == 6:
        with open(Q6_OUTPUT_DATA_FILE, 'r') as f:
            while True:
                socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
                line = f.readline()
                if line:
                    line_list = line.strip().lstrip('(').rstrip(')').split(',')
                    logging.info("send: " + str(line_list))
                    socketio.emit('result_figure_data', {'data': line_list})
                else:
                    # f.seek(0)
                    break
    elif query_idx == 3:
        with open(Q3_OUTPUT_DATA_FILE, 'r') as f:
            while True:
                socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
                line = f.readline()
                if line:
                    line_list = line.strip().lstrip('(').rstrip(')').split(',')
                    logging.info("send: " + str(line_list))
                    socketio.emit('result_figure_data', {'data': line_list})
                else:
                    # f.seek(0)
                    break
    else:
        logging.error("query index " + query_idx + " is not supported.")


def background_send_kafka_data_thread_real():
    KAFKA_HOME_PATH = '/Users/chaoqi/Programs/kafka_2.12-2.6.0'
    KAFKA_CONSUMER_BOOTSTRAP_SERVERS = 'localhost:9092'
    KAFKA_CONSUMER_GROUP_ID = 'aju_generated_jar_output_group'
    KAFKA_CONSUMER_TOPIC = 'aju_generated_jar_output'
    KAFKA_CONSUMER_AUTO_OFFSET_RESET = 'earliest'
    SERVER_SEND_DATA_TO_CLIENT_INTEVAL = 0.1

    kafka_consumer = Consumer({
        'bootstrap.servers': KAFKA_CONSUMER_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_CONSUMER_GROUP_ID,
        'auto.offset.reset': KAFKA_CONSUMER_AUTO_OFFSET_RESET
    })
    kafka_consumer.subscribe([KAFKA_CONSUMER_TOPIC])

    while True:
        socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
        msg = kafka_consumer.poll(1)
        if msg:
            msg_list = msg.value().decode('utf-8').strip().lstrip('(').rstrip(')').split(',')
            logging.info("send: ", str(msg_list))
            socketio.emit('result_figure_data', {'data': msg_list})
