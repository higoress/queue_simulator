from main import Controler
from flask import Flask, request

app = Flask(__name__)

controler = Controler()
controler.start(0,2,0,4,1)

@app.route('/clients')
def get_clients():
    return controler.get_status()

@app.route('/start', methods=['POST'])
def start():
    if request.method == 'POST' :
        contoler.start(0,2,0,4,3)
        return 'System started'

