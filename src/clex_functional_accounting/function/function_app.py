# function_app.py

from flask import Flask, Response, render_template_string
from clex_functional_accounting.lib import config,cosmosdb
import azure.functions as func
import asyncio
from datetime import datetime

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
                <th>SU Usage</th>
                <th>SU Grant</th>
                <th>Scratch Usage</th>
                <th>Scratch Grant</th>
                <th>Scratch iUsage</th>
                <th>Scratch iGrant</th>
                <th>Gdata Usage</th>
                <th>Gdata Grant</th>
                <th>Gdata iUsage</th>
                <th>Gdata iGrant</th>
                <th>Massdata Usage</th>
                <th>Massdata Grant</th>
                <th>Massdata iUsage</th>
                <th>Massdata iGrant</th>
            </tr>
        </thead>
        <tbody>
            {% for row in compute_data %}
            <tr>
                <td>
                    {{row.proj}}
                </td>
                <td>
                    {{row.compute_total}}
                </td>
                <td>
                    {{row.compute_grant}}
                </td>
                <td>
                    {{row.scratch_usage}}
                </td>
                <td>
                    {{row.scratch_quota}}
                </td>
                <td>
                    {{row.scratch_iusage}}
                </td>
                <td>
                    {{row.scratch_iquota}}
                </td>
                <td>
                    {{row.gdata_usage}}
                </td>
                <td>
                    {{row.gdata_quota}}
                </td>
                <td>
                    {{row.gdata_iusage}}
                </td>
                <td>
                    {{row.gdata_iquota}}
                </td>
                <td>
                    {{row.massdata_usage}}
                </td>
                <td>
                    {{row.massdata_quota}}
                </td>
                <td>
                    {{row.massdata_iusage}}
                </td>
                <td>
                    {{row.massdata_iquota}}
                </td>
            </tr>
            {%endfor%}
        </tbody>
    </table>
</html>
'''

flask_app = Flask(__name__)

async def return_http(username: str):

    storage_list=("scratch","gdata","massdata")
    storage_params=("quota","usage","iquota","iusage")

    writer = cosmosdb.CosmosDBWriter()
    compute_future = writer.get_container("compute","Accounting",quarterly=True)
    storage_future = writer.get_container("storage","Accounting",quarterly=True)
    _ = await writer.get_container("users","Accounting")
    user_d = await writer.read_items("users",username,partition_key_val='gadi',once_off=True)
    if not user_d:
        compute_future.cancel()
        storage_future.cancel()
        await writer.close()
        return Response("Invalid User",404)

    proj_list = user_d[0]['groups']

    compute_data=[]
    keys=['compute_grant','compute_total']
    for i in storage_list:
        keys.extend([ f"{i}_{j}" for j in storage_params ])

    await asyncio.gather(compute_future,storage_future)
    futures=[]

    for proj in proj_list:
        futures.append(writer.query("compute",["project","user","usage"],[f"project='{proj}'","user IN ('grant','total')"],'ts DESC',0,2))
        futures.append(writer.query("storage",["project","fs","usage","quota","iusage","iquota"],f"project='{proj}'",'ts DESC',0,3))

    all_query_results = await asyncio.gather(*futures)

    for query_compute, query_storage in zip(all_query_results[0::2],all_query_results[1::2]):
        row = dict.fromkeys(keys,0)
        if query_compute:
            for d in query_compute:
                row['compute_'+d['user']] = d['usage']
        if query_storage:
            seen = dict.fromkeys(storage_list,False)
            for d in query_storage:
                fs=d['fs']
                if not seen[fs]:
                    seen[fs] = True
                    for k in storage_params:
                        row[f"{fs}_{k}"] = d[k]
        if all([i==0 for i in row.values()]):
            continue
        row['proj']=query_compute[0]['project']
        compute_data.append(row)

    resp = Response(render_template_string(TEMPLATE,compute_data=compute_data,user={'firstname':user_d[0]['pw_name'].split()[0],'groups':proj_list}))

    await writer.close()

    return resp

@flask_app.get("/demo/<username>")
def async_return_http(username):
    return asyncio.run(return_http(username))

app = func.WsgiFunctionApp(app=flask_app.wsgi_app, 
                           http_auth_level=func.AuthLevel.ANONYMOUS)
