# function_app.py

import azure.functions as func 
from flask import Flask, Response 

flask_app = Flask(__name__) 

@flask_app.get("/return_http") 
def return_http(): 
    return Response("<h1>Hello World™</h1>", mimetype="text/html") 

app = func.WsgiFunctionApp(app=flask_app.wsgi_app, 
                           http_auth_level=func.AuthLevel.ANONYMOUS)
