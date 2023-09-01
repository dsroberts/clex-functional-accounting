# function_app.py

from flask import Flask, Response, render_template_string
from clex_functional_accounting.lib import config,cosmosdb
import azure.functions as func

TEMPLATE='''
<!DOCTYPE html>
<html>
    <head>
        <title>Title Here</title>
    </head>
    <h2>Hello {{user.firstname}}</h2>
    <br>
    You are a member of the following projects
    <ul>
    {% for proj in user.groups %}
    <li>{{proj}}</li>
    {% endfor %}
    </ul>
    <br>

    Current resource usage for your projects:
    <table>
        <thead>
            <tr>
                <th>Project</th>
                <th>SU Used</th>
                <th>SU Grant</th>
            </tr>
        </thead>
        <tbody>
            {% for row in compute_data %}
            <tr>
                <td>
                    {{row.proj}}
                </td>
                <td>
                    {{row.used}}
                </td>
                <td>
                    {{row.grant}}
                </td>
            </tr>
            {%endfor%}
        </tbody>
    </table>
</html>
'''

flask_app = Flask(__name__) 

@flask_app.get("/demo/<username>")
def return_http(username: str):

    writer = cosmosdb.CosmosDBWriter()
    _ = writer.get_container("users","Accounting")
    user_d = writer.read_items("users",username,partition_key_val='gadi',once_off=True)
    if not user_d:
        return Response("Invalid User",404)

    proj_list = user_d[0]['groups']

    _ = writer.get_container("compute","Accounting",quarterly=True)
    compute_data=[]
    for proj in proj_list:
        query_out = writer.query("compute",["user","usage"],[f"project='{proj}'","user IN ('grant','total')"],'ts',0,2)
        if not query_out:
            continue
        for d in query_out:
            if d['user'] == 'grant':
                grant=d['usage']
            if d["user"] == 'total':
                total=d['usage']
        if grant == 0:
            continue
        compute_data.append({"proj":proj,"used":total,"grant":grant})

    resp = Response(render_template_string(TEMPLATE,compute_data=compute_data,user={'firstname':user_d[0]['pw_name'].split()[0],'groups':proj_list}))

    return resp

app = func.WsgiFunctionApp(app=flask_app.wsgi_app, 
                           http_auth_level=func.AuthLevel.ANONYMOUS)
