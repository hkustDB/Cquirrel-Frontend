import os

# path settings
FLINK_HOME_PATH = os.environ.get('FLINK_HOME') or '/Users/chaoqi/Programs/flink-1.11.2'

GUI_FLASK_PATH = os.path.abspath(os.path.dirname(__file__))
JSON_FILE_UPLOAD_PATH = os.path.join(GUI_FLASK_PATH, 'uploads')
GENERATED_JAR_PATH = os.path.join(GUI_FLASK_PATH, 'jar')
GENERATED_CODE_DIR = os.path.join(GENERATED_JAR_PATH, 'generated-code')
GENERATED_JAR_FILE = os.path.join(GUI_FLASK_PATH, 'jar/generated-code/target/generated-code-1.0-SNAPSHOT-jar-with-dependencies.jar')
CODEGEN_FILE = os.path.join(GUI_FLASK_PATH, 'jar/codegen.jar')
CODEGEN_LOG_PATH = os.path.join(GUI_FLASK_PATH, 'log')
CODEGEN_LOG_FILE = os.path.join(GUI_FLASK_PATH, 'log/codegen.log')
TEST_RESOURCES_PATH = os.path.join(GUI_FLASK_PATH, 'tests/resources')
INPUT_DATA_FILE = os.path.join(GUI_FLASK_PATH, 'aju_app/resources/input_data_q6_all_insert.csv')
OUTPUT_DATA_FILE = os.path.join(GUI_FLASK_PATH, 'aju_app/resources/output_data_q6_all_insert.csv')

class BaseConfig:
    pass

    @staticmethod
    def init_app(app):
        pass


class DevelopmentConfig(BaseConfig):
    DEBUG = True


class TestingConfig(BaseConfig):
    TESTING = True


class ProductionConfig(BaseConfig):
    pass

    @classmethod
    def init_app(cls, app):
        BaseConfig.init_app(app)

        import logging


class DockerConfig(ProductionConfig):
    @classmethod
    def init_app(cls, app):
        ProductionConfig.init_app(app)

        # log to stderr
        import logging
        from logging import StreamHandler
        file_handler = StreamHandler()
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)


class UnixConfig(ProductionConfig):
    @classmethod
    def init_app(cls, app):
        ProductionConfig.init_app(app)

        # log to syslog
        import logging
        from logging.handlers import SysLogHandler
        syslog_handler = SysLogHandler()
        syslog_handler.setLevel(logging.INFO)
        app.logger.addHandler(syslog_handler)


config_options = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'docker': DockerConfig,
    'unix': UnixConfig,

    'default': DevelopmentConfig
}
