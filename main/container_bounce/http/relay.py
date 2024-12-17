import os
from pathlib import Path
from datetime import datetime as dt
import json
from flask import Flask, request, make_response
from gevent.pywsgi import WSGIServer

HERE = Path(__file__).resolve().parent

app = Flask(__name__, static_url_path='', static_folder='public')
def getSecret():
    secret_path = HERE/".secret"
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

API = Path("/api/v1")

def Timestamp(timestamp: dt|None = None):
    ts = dt.now() if timestamp is None else timestamp
    FORMAT = '%Y-%m-%d_%H-%M-%S'
    return f"{ts.strftime(FORMAT)}"

@app.route("/", methods=['POST'])
def home():
    res = make_response("this is intended for api", 200)
    res.mimetype = "text/plain"
    return res

queue = []
@app.route(str(API/"sbatch"), methods=['POST'])
def sbatch():
    if not isinstance(request.json, dict): return "no data", 400
    cmd = request.json.get("command", "")
    queue.append((Timestamp(), cmd))
    res = make_response(f"[{len(queue)}] commands queued", 200)
    res.mimetype = "text/plain"
    return res

@app.route(str(API/"handle_sbatch"), methods=['GET'])
def handle_sbatch():
    res = make_response(queue, 200)
    queue.clear()
    res.mimetype = "application/json"
    return res

if __name__ == '__main__':
    # http_server = WSGIServer(("0.0.0.0", 443), app)
    http_server = WSGIServer(("localhost", 56101), app)
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        pass
