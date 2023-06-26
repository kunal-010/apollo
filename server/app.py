from flask import Flask
app = Flask(__name__)

def customFunction():
    return "Hello World"

@app.route('/runScript', methods = ['GET'])
def runScript():
    return customFunction()

if __name__ == '__main__':
  
    app.run(debug = True)

