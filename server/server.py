from flask import Flask,render_template,request, make_response, send_file
from apollo_proxy import get_csv_from_url
import pandas as pd
from waitress import serve
from flask_cors import CORS
import jsonpickle

application = Flask(__name__)
CORS(application)

@application.route('/run', methods = ['POST', 'GET'])
def data():
    
    if request.method == 'GET':
        return render_template('form.html')
    
    if request.method == 'POST':
        form_data = request.get_json()
        
        df = get_csv_from_url(form_data["url"], int(form_data["records"][:-1])*1000).astype(str)
        def generate():
            for row in df.to_csv(index=False).split("\r\n"):
                yield row + "\r\n"
        headers = {
            "Access-Control-Allow-Origin" : "*",
            "Access-Control-Allow-Headers" : "*",
            "Access-Control-Allow-Methods" : "*",
            "Content-Disposition" : "attachment; filename=Apollo_Data.csv", 
            "Content-Type": "text/csv"
        }
        return jsonpickle.encode(generate()), headers

if __name__ == "__main__":
    application.run(host='0.0.0.0', port='8080', debug = True)