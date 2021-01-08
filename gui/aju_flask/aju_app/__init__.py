from confluent_kafka import Consumer
import os
import logging
import threading

from flask import Flask
from flask_bootstrap import Bootstrap
from flask_socketio import SocketIO
from flask_cors import CORS
from flask import current_app

from config import config_options
from config import Q6_OUTPUT_DATA_FILE
from config import Q3_OUTPUT_DATA_FILE
from config import Q10_OUTPUT_DATA_FILE

from aju_app import aju_utils

bootstrap = Bootstrap()
# socketio = SocketIO()
# socketio = SocketIO(cors_allowed_origins="*", cors_credentials=False, async_mode='eventlet')
# socketio = SocketIO(cors_allowed_origins="*", cors_credentials=False, async_mode='threading')
socketio = SocketIO(cors_allowed_origins="*", cors_credentials=False, ping_timeout=50000)

# import eventlet
# eventlet.monkey_patch()

cors = CORS(resources={r"/*": {"origins": "*"}})

stop_send_data_thread_flag = False


def create_app(config_name):
    aju_utils.clean_flink_output_files()

    app = Flask(__name__)
    app.secret_key = os.urandom(24)
    app.config.from_object(config_options[config_name])
    config_options[config_name].init_app(app)
    app.config['CORS_HEADERS'] = 'Content-Type'

    bootstrap.init_app(app)
    socketio.init_app(app)
    cors.init_app(app)

    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    from .r import r as r_blueprint
    app.register_blueprint(r_blueprint)

    return app


@socketio.on('connect')
def socketio_connect():
    print("socketio connected")


@socketio.on('disconnect')
def socketio_disconnect():
    print('socketio disconnected')


@socketio.on('r_stop_send_data', namespace='/ws')
def r_stop_server_send_data_thread(data):
    global stop_send_data_thread_flag
    stop_send_data_thread_flag = True


def send_query_result_data_to_client(query_idx):
    global stop_send_data_thread_flag
    stop_send_data_thread_flag = False
    if query_idx == 3:
        logging.info("sending query " + str(query_idx) + " result data to client...")
        t = threading.Thread(target=send_query_result_data_file_q3, args=(Q3_OUTPUT_DATA_FILE,))
        t.start()
    elif query_idx == 6:
        logging.info("sending query " + str(query_idx) + " result data to client...")
        t = threading.Thread(target=send_query_result_data_file_q6, args=(Q6_OUTPUT_DATA_FILE,))
        t.start()
    else:
        logging.error("query " + str(query_idx) + " does not support for now.")


def send_query_result_data_file_q3(filepath):
    SERVER_SEND_DATA_TO_CLIENT_INTEVAL = 0.001
    socketio.emit('start_figure_data_transmit', {'data': 1})

    # tmp set
    line_list_len = 9
    aggregate_name_idx = 7

    x_timestamp_idx = line_list_len - 1
    y_value_idx = int((aggregate_name_idx - 1) / 2)
    attribute_length = int((line_list_len - 1) / 2)

    total_data = {}
    x_timestamp = []
    max_record = {}

    with open(filepath, 'r') as f:
        while True:
            global stop_send_data_thread_flag
            if stop_send_data_thread_flag:
                break
            socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
            line = f.readline()
            if line:
                line_list = line.strip().lstrip('(').rstrip(')').split(',')
                for i in range(len(line_list)):
                    line_list[i] = line_list[i].strip()

                #
                # top 5 query according to revenue
                #
                N = 5

                # get current key_tag
                key_tag = ""
                for i in range(attribute_length):
                    if i == y_value_idx:
                        continue
                    key_tag = key_tag + line_list[attribute_length + i] + ":" + line_list[i] + ","
                key_tag = key_tag[: (len(key_tag) - 1)]

                # add the new value into total_data
                if key_tag not in total_data:
                    # if total_data is not null, in each key, add the last value
                    if len(total_data) != 0:
                        # add other key_tag
                        for key in total_data:
                            tmpValue = total_data.get(key)
                            total_data[key] = [x for x in tmpValue] + [tmpValue[-1]]
                    # add the new key_tag
                    total_data[key_tag] = []
                    for i in range(len(x_timestamp)):
                        total_data[key_tag].append(0.0)
                    total_data[key_tag].append(float(line_list[y_value_idx]))
                else:
                    for key in total_data:
                        tmpValue = total_data.get(key)
                        total_data[key] = [x for x in tmpValue] + [tmpValue[-1]]
                    total_data[key_tag].pop(len(total_data[key_tag]) - 1)
                    total_data[key_tag].append(float(line_list[y_value_idx]))

                # add timestamp
                x_timestamp.append(line_list[x_timestamp_idx])
                # update the max condition
                max_record[key_tag] = max(total_data[key_tag])

                # get top N key_tag
                topN = sorted(max_record.items(), key=lambda item: item[1], reverse=True)
                topN = topN[:N]
                top_value_data = {}
                for k, v in topN:
                    top_value_data[k] = total_data[k]

                logging.info("send: " + str(line_list))
                socketio.emit('result_figure_data',
                              {'queryNum': 3,
                               'data': line_list,
                               'x_timestamp': x_timestamp,
                               "top_value_data": top_value_data})
            else:
                break


def send_query_result_data_file_q6(filepath):
    SERVER_SEND_DATA_TO_CLIENT_INTEVAL = 0.001
    socketio.emit('start_figure_data_transmit', {'data': 1})
    with open(filepath, 'r') as f:
        while True:
            global stop_send_data_thread_flag
            if stop_send_data_thread_flag:
                break
            socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
            line = f.readline()
            if line:
                line_list = line.strip().lstrip('(').rstrip(')').split(',')
                logging.info("send: " + str(line_list))
                socketio.emit('result_figure_data', {'queryNum': 6, 'data': line_list})
            else:
                break


def send_query_result_data_file(filepath):
    SERVER_SEND_DATA_TO_CLIENT_INTEVAL = 0.001
    socketio.emit('start_figure_data_transmit', {'data': 1})
    with open(filepath, 'r') as f:
        while True:
            global stop_send_data_thread_flag
            if stop_send_data_thread_flag:
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
    global stop_send_data_thread_flag
    stop_send_data_thread_flag = True


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


def r_send_codgen_log_and_retcode(codegen_log, retcode):
    socketio.start_background_task(target=_send_codgen_log_and_retcode, codegen_log=codegen_log, retcode=retcode)


def _send_codgen_log_and_retcode(codegen_log, retcode):
    socketio.sleep(0.001)
    socketio.emit('r_codegen_log', {"codegen_log": codegen_log, "retcode": retcode}, namespace='/ws')


def r_set_step_to(n):
    socketio.start_background_task(target=_set_step_to, n=n)


def _set_step_to(n):
    socketio.sleep(0.001)
    print("r_set_step_to: ", n)
    socketio.emit('r_set_step', {"step": n}, namespace='/ws')


def r_send_message(m_type, message):
    socketio.start_background_task(target=_send_message, m_type=m_type, message=message)


def _send_message(m_type, message):
    socketio.sleep(0.001)
    socketio.emit('r_message', {"m_type": m_type, "message": message}, namespace='/ws')


def r_send_query_result_data_to_client(query_idx):
    global stop_send_data_thread_flag
    stop_send_data_thread_flag = False
    if query_idx == 3:
        logging.info("sending query " + str(query_idx) + " result data to client...")
        r_send_query_result_data_file_q3(Q3_OUTPUT_DATA_FILE)
    elif query_idx == 6:
        logging.info("sending query " + str(query_idx) + " result data to client...")
        r_send_query_result_data_file_q6(Q6_OUTPUT_DATA_FILE)
    elif query_idx == 10:
        logging.info("sending query " + str(query_idx) + " result data to client...")
        r_send_query_result_data_file_q10(Q10_OUTPUT_DATA_FILE)
    else:
        logging.error("query " + str(query_idx) + " does not support for now.")


def r_send_query_result_data_file_q6(filepath):
    SERVER_SEND_DATA_TO_CLIENT_INTEVAL = 0.0001
    print("r_send_query_result_data_file_q6: ", "start")
    socketio.emit('r_start_to_send_data', {"status": "start"}, namespace='/ws')
    with open(filepath, 'r') as f:
        while True:
            global stop_send_data_thread_flag
            if stop_send_data_thread_flag:
                break
            line = f.readline()
            if line:
                # print("r_send_query_result_data_file_q6: ", line)
                line = line.strip('\x00')
                line_list = line.strip().lstrip('(').rstrip(')').split(',')
                logging.info("send: " + str(line_list))
                if len(line_list) == 3:
                    # print("r_figure_data: ", str(line_list))
                    socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
                    socketio.emit('r_figure_data', {"queryNum": 6, "data": line_list}, namespace='/ws')
            else:
                r_set_step_to(5)
                break


def r_send_query_result_data_file_q3(filepath):
    SERVER_SEND_DATA_TO_CLIENT_INTEVAL = 0.0001
    socketio.emit('r_start_to_send_data', {"status": "start"}, namespace='/ws')

    # tmp set
    line_list_len = 9
    aggregate_name_idx = 7

    x_timestamp_idx = line_list_len - 1
    y_value_idx = int((aggregate_name_idx - 1) / 2)
    attribute_length = int((line_list_len - 1) / 2)

    total_data = {}
    x_timestamp = []
    max_record = {}

    with open(filepath, 'r') as f:
        while True:
            global stop_send_data_thread_flag
            if stop_send_data_thread_flag:
                break
            socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
            line = f.readline()
            if line:
                line_list = line.strip().lstrip('(').rstrip(')').split(',')
                for i in range(len(line_list)):
                    line_list[i] = line_list[i].strip()

                #
                # top 5 query according to revenue
                #
                N = 5

                # get current key_tag
                key_tag = ""
                for i in range(attribute_length):
                    if i == y_value_idx:
                        continue
                    key_tag = key_tag + line_list[attribute_length + i] + ":" + line_list[i] + ","
                key_tag = key_tag[: (len(key_tag) - 1)]

                # add the new value into total_data
                if key_tag not in total_data:
                    # if total_data is not null, in each key, add the last value
                    if len(total_data) != 0:
                        # add other key_tag
                        for key in total_data:
                            tmpValue = total_data.get(key)
                            total_data[key] = [x for x in tmpValue] + [tmpValue[-1]]
                    # add the new key_tag
                    total_data[key_tag] = []
                    for i in range(len(x_timestamp)):
                        total_data[key_tag].append(0.0)
                    total_data[key_tag].append(float(line_list[y_value_idx]))
                else:
                    for key in total_data:
                        tmpValue = total_data.get(key)
                        total_data[key] = [x for x in tmpValue] + [tmpValue[-1]]
                    total_data[key_tag].pop(len(total_data[key_tag]) - 1)
                    total_data[key_tag].append(float(line_list[y_value_idx]))

                # add timestamp
                x_timestamp.append(line_list[x_timestamp_idx])
                # update the max condition
                max_record[key_tag] = max(total_data[key_tag])

                # get top N key_tag
                topN = sorted(max_record.items(), key=lambda item: item[1], reverse=True)
                topN = topN[:N]
                top_value_data = {}
                for k, v in topN:
                    top_value_data[k] = total_data[k]

                logging.info("send: " + str(line_list))
                socketio.emit('r_figure_data',
                              {'queryNum': 3,
                               'data': line_list,
                               'x_timestamp': x_timestamp,
                               "top_value_data": top_value_data}, namespace='/ws')
            else:
                r_set_step_to(5)
                break


def r_send_query_result_data_file_q10(filepath):
    SERVER_SEND_DATA_TO_CLIENT_INTEVAL = 0.0001
    print("r_send_query_result_data_file_q10: ", "start")
    socketio.emit('r_start_to_send_data', {"status": "start"}, namespace='/ws')
    with open(filepath, 'r') as f:
        while True:
            global stop_send_data_thread_flag
            if stop_send_data_thread_flag:
                break
            line = f.readline()
            if line:
                # print("r_send_query_result_data_file_q6: ", line)
                line = line.strip('\x00')
                line_list = line.strip().lstrip('(').rstrip(')').split(',')
                logging.info("send: " + str(line_list))
                # if len(line_list) == 3:
                    # print("r_figure_data: ", str(line_list))
                socketio.sleep(SERVER_SEND_DATA_TO_CLIENT_INTEVAL)
                socketio.emit('r_figure_data', {"queryNum": 10, "data": line_list}, namespace='/ws')
            else:
                r_set_step_to(5)
                break
