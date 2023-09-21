# function_app.py

from flask import Flask, Response, render_template_string
from clex_functional_accounting.lib import cosmosdb,blob,group_list
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

@flask_app.get("/demo/<username>")
def return_http(username: str):

    storage_list=("scratch","gdata","massdata")
    storage_params=("quota","usage","iquota","iusage")

    my_groups = group_list.get_group_list()

    db_writer = cosmosdb.CosmosDBWriter()
    blob_writer = blob.BlobWriter()
    _ = db_writer.get_container("compute","Accounting",quarterly=True)
    _ = db_writer.get_container("storage","Accounting",quarterly=True)

    user_d = blob_writer.read_item(blob.CONTAINER,'users')

    if username not in user_d:
        return Response("Invalid User",404)

    proj_list = [ proj for proj in user_d[username]['groups'] if proj in my_groups ]

    keys=['compute_grant','compute_total']
    for i in storage_list:
        keys.extend([ f"{i}_{j}" for j in storage_params ])

    query_projs=', '.join([ "'" + proj + "'" for proj in proj_list ])
    compute_queries = db_writer.query("compute",["project","user","usage"],[f"project in ({query_projs})","user IN ('grant','total')"],'ts DESC',0,2*len(proj_list))
    storage_queries = db_writer.query("storage",["project","fs","usage","quota","iusage","iquota"],f"project in ({query_projs})",'ts DESC',0,3*len(proj_list))

    ### Construct all keys we're expecting
    seen_keys=[]
    for i in keys:
        seen_keys.extend([ f"{i}_{proj}" for proj in proj_list ])
    seen_data=dict.fromkeys(seen_keys,False)

    data_d={}
    for compute_query in compute_queries:
        seen_key = f"compute_{compute_query['user']}_{compute_query['project']}"
        if seen_data[seen_key]:
            continue
        seen_data[seen_key] = True
        if compute_query['project'] in data_d:
            row = data_d[compute_query['project']]
        else:
            row = dict.fromkeys(keys,0)
        row['compute_'+compute_query['user']] = compute_query['usage']
        data_d[compute_query['project']] = row
    
    for storage_query in storage_queries:
        seen_key = f"{storage_query['fs']}_quota_{storage_query['project']}"
        if seen_data[seen_key]:
            continue
        seen_data[seen_key] = True
        if storage_query['project'] in data_d:
            row = data_d[storage_query['project']]
        else:
            row = dict.fromkeys(keys,0)
        for k in storage_params:
            row[f"{storage_query['fs']}_{k}"] = storage_query[k]
        data_d[storage_query['project']] = row

    compute_data=[]
    for k,v in data_d.items():
        compute_data.append({'proj':k} | v )


    #for query_compute, query_storage in zip(all_query_results[0::2],all_query_results[1::2]):
    #    row = dict.fromkeys(keys,0)
    #    if query_compute:
    #        for d in query_compute:
    #            row['compute_'+d['user']] = d['usage']
    #    if query_storage:
    #        seen = dict.fromkeys(storage_list,False)
    #        for d in query_storage:
    #            fs=d['fs']
    #            if not seen[fs]:
    #                seen[fs] = True
    #                for k in storage_params:
    #                    row[f"{fs}_{k}"] = d[k]
    #    if all([i==0 for i in row.values()]):
    #        continue
    #    row['proj']=query_compute[0]['project']
    #    compute_data.append(row)


    resp = Response(render_template_string(TEMPLATE,compute_data=compute_data,user={'firstname':user_d[username]['pw_name'].split()[0],'groups':user_d[username]['groups']}))

    return resp

app = func.WsgiFunctionApp(app=flask_app.wsgi_app, 
                           http_auth_level=func.AuthLevel.ANONYMOUS)
