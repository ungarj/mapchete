#!/usr/bin/env python

from flask import Flask

app = Flask(__name__)


@app.route('/', methods=['GET'])
def get_tasks():
    return "herbert"

@app.route('/wmts_simple', methods=['GET'])
def get_getcapabilities():
    return "getcapabilities"

@app.route('/wmts_simple/<string:process_id>', methods=['GET'])
def get_processname(process_id):
    return process_id

@app.route('/wmts_simple/<string:process_id>/<int:zoom>/<int:row>/<int:col>.tif', methods=['GET'])
def get_tile(process_id, zoom, row, col):
    # return str(zoom), str(row), str(col)
    tileindex = str(zoom), str(row), str(col)
    return str(tileindex)


if __name__ == '__main__':
    app.run(debug=True)
