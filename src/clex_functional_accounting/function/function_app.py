# function_app.py

import azure.functions as func 
import azure.cosmos.cosmos_client as cosmos_client

from flask import Flask, Response, render_template_string
import os

TEMPLATE='''
<!DOCTYPE html>
<html>
    <head>
        <title>Title Here</title>
    </head>
    <h2>Hello {{data.user.firstname}}</h2>
    <br>
    You are a member of the following projects
    <ul>
    {% for proj in data %}
    <li>proj.proj_id</li>
    {% endfor %}
</html>
'''

flask_app = Flask(__name__) 

@flask_app.get("/demo") 
def return_http(): 

    ev = os.getenv("AzureWebJobsStorage","Didn't work")
    return Response(f"<h1>Hello World™ {ev}</h1>", mimetype="text/html")
    #resp = Response(render_template_string(TEMPLATE,data=))

app = func.WsgiFunctionApp(app=flask_app.wsgi_app, 
                           http_auth_level=func.AuthLevel.ANONYMOUS)
