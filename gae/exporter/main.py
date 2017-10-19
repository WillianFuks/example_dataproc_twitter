from flask import Flask, render_template, request
from beam_jobs.run import export

app = Flask(__name__)

@app.route("/test")
def test():
    return "it's working"

@app.route("/export_customers")
def export_customers():
    export()
    
