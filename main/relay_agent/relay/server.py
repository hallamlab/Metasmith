import os, sys
from dataclasses import dataclass, field, fields
from pathlib import Path
from flask import Flask
from enum import Enum
from flask_socketio import SocketIO, send, emit
import gevent
import signal

class SERVER_HEALTH(Enum):
    DEAD = 0
    ALIVE = 1
    STALE = 2

@dataclass
class ServerStatus:
    health: SERVER_HEALTH

def CreateServer():
    app = Flask(__name__, static_url_path='', static_folder='public')

    HERE = Path(__file__).parent
    def getSecret():
        secret_path = HERE/'secrets'
        os.system(f'mkdir -p {secret_path}')
        secret_path = secret_path/'secret'
        try:
            with open(secret_path, 'r') as s:
                return s.readlines()[0][:-1]
        except FileNotFoundError:
            import secrets
            with open(secret_path, 'w') as s:
                tok = secrets.token_urlsafe(64)
                s.write(tok)
                s.flush()
                return tok
    app.config['SECRET_KEY'] = getSecret()
    socketio = SocketIO(app, async_mode='gevent')
    def on_term_signal():
        exit(0)
    gevent.signal_handler(signal.SIGTERM, on_term_signal)
    # @app.route('/')
    # def Home():
    #     return "hello"

    @socketio.on('echo')
    def handle_echo(data):
        emit('echo', dict(echo=data), json=True)

# URL_ROOT = 'http://localhost:12001'
# API_PREFIX = 'api/v1'

