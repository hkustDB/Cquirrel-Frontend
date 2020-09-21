from flask import Flask
from flask_bootstrap import Bootstrap
from flask_socketio import SocketIO

from config import config_options

bootstrap = Bootstrap()
socketio = SocketIO()


def create_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config_options[config_name])
    config_options[config_name].init_app(app)

    bootstrap.init_app(app)
    socketio.init_app(app)

    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    return app
