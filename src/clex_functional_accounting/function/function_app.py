# function_app.py

from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException, NotFound
from clex_functional_accounting.lib import cosmosdb,blob,group_list
import json
from datetime import datetime, timedelta

import azure.functions as func
from typing import Any, Dict, List, Union

one_hour=timedelta(hours=1)

STANDARD_HEADERS= {
        'Access-Control-Allow-Origin': '*'
    }

SUBSTRING_MATCH_FIELDS=[ 'id', 'pw_name' ]
INTEGER_FIELDS=[ 'uid', 'gid' ]

class Datetime_with_quarter(datetime):
    def quarter(self):
        return f"{self.year}.q{(self.month-1)//3+1}"


def content_range_headers(who: str, start: int, end: int, total: int):
    return {
        'Content-Range': f"{who} {start}-{end}/{total}",
        'Access-Control-Expose-Headers': 'Content-Range'
    }

def field_match(field: str, a: Union[str,int], b: Union[str,int] ) -> bool:
    if field in SUBSTRING_MATCH_FIELDS:
        return any( i.lower() in a.lower() for i in b )
    return any( a == i for i in b )

def filter_list(in_list: List[Dict[str,Any]],filt:Dict[str,Union[str,int,List]]):
    if not in_list:
        return in_list
    ### Don't bother if none of the keys exist in the output
    out_list=[]
    if any( k in in_list[0] for k in filt ):
        real_filt = { k:v for k,v in filt.items() if k in in_list[0] }
        ### Handle the case where we're filtering on a single value
        for k,v in real_filt.items():
            ### Can have a pretty small list of integer conversions given this
            ### function is only for users and groups
            if k in INTEGER_FIELDS: v = int(v)
            if isinstance(v,str) or isinstance(v,int): real_filt[k] = [v,]
        for l in in_list:
            #if all( l[k] in v for k,v in real_filt.items() ):
            if all( field_match(k,l[k],v) for k,v in real_filt.items() ):
                out_list.append(l)
        return out_list
    else:
        return in_list

def remove_internal_data(in_list: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    return [ {k:v for k,v in i.items() if not k[0] == '_' } for i in in_list ]

def remove_internal_data_single(in_dict: Dict[str,Any]) -> Dict[str,Any]:
    return {k:v for k,v in in_dict.items() if not k[0] == '_' }

def sanitize_time(time: str) -> datetime:
    if time.endswith('Z'):
        return Datetime_with_quarter.fromisoformat(time[:-1])
    return Datetime_with_quarter.fromisoformat(time)


class AccountingAPI(object):
    def __init__(self,config):

        url_map = []
        for func in dir(self):
            if func.startswith("api_get_"):
                url_map.append(Rule(f'/api/v0/{func.split("_",maxsplit=2)[-1]}',endpoint=func))
                url_map.append(Rule(f'/api/v0/{func.split("_",maxsplit=2)[-1]}/<param>',endpoint=func))
        
        self.url_map = Map(url_map)

    def error_404(self):
        return Response("{'data': 'Not Found'}",404,content_type="application/json")

    def error_400(self):
        return Response("{'data': 'Invalid'}",400,content_type="application/json")

    def wsgi_app(self, environ, start_response):
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)
    
    def __call__(self, environ, start_response: Response):
        return self.wsgi_app(environ, start_response)

    def dispatch_request(self, request: Request):
        adapter = self.url_map.bind_to_environ(request.environ)
        
        try:
            endpoint, values = adapter.match()
        except NotFound:
            return self.error_404()
        except HTTPException as e:
            return e
        
        try:
            return getattr(self,endpoint)(request,**values)
        except NotFound:
            return self.error_404()
        except HTTPException as e:
            return e
        
    ### This function handles getOne, getMany, getManyReference and getList
    def api_get_users(self,request: Request,param=None):

        blob_writer = blob.BlobWriter()
        user_d = blob_writer.read_item(blob.CONTAINER,'users')
        monitored_projects = blob_writer.read_item(blob.CONTAINER,'projectlist')

        if param:
            try:
                user_d[param]['groups'] = sorted(list(set(user_d[param]["groups"]) & set(monitored_projects)))
                return Response(json.dumps({"id":param} | user_d[param]),content_type="application/json")
            except KeyError:
                return Response('{}',content_type="application/json")

        all_user_list=[ {"id":k} | v for k,v in user_d.items() ]
        out_l=all_user_list
        headers=STANDARD_HEADERS

        ### Filter
        if "filter" in request.args:
            filt=json.loads(request.args.get("filter"))
            out_l = filter_list(all_user_list,filt)

        ### Sort
        if "sort" in request.args:
            field,order = json.loads(request.args.get("sort"))
            out_l=sorted(out_l,key=lambda x: x[field],reverse=(order=="DESC"))

        ### Pagination
        if "range" in request.args:
            start, end = json.loads(request.args["range"])
            total = len(out_l)
            end=min(end,total-1)
            out_l=out_l[start:end+1]
            headers = headers | content_range_headers("users",start,end,total)

        for i in out_l:
            i["groups"] = sorted(list(set(i["groups"]) & set(monitored_projects)))

        return Response(json.dumps(out_l),content_type="application/json",headers=headers)
    
    def api_get_groups(self,request: Request,param=None):

        blob_writer = blob.BlobWriter()
        group_d = blob_writer.read_item(blob.CONTAINER,'groups')
        monitored_projects = blob_writer.read_item(blob.CONTAINER,'projectlist')

        if param:
            if param in monitored_projects:
                return Response(json.dumps({"id":param} | group_d[param]),content_type="application/json")
            else:
                return Response('{}',content_type="application/json")

        all_group_list=[ {"id":k} | v for k,v in group_d.items() if k in monitored_projects ]
        out_l=all_group_list
        headers=STANDARD_HEADERS

        ### Filter
        if "filter" in request.args:
            filt=json.loads(request.args.get("filter"))
            out_l = filter_list(all_group_list,filt)

        ### Sort
        if "sort" in request.args:
            field,order = json.loads(request.args.get("sort"))
            out_l=sorted(out_l,key=lambda x: x[field],reverse=(order=="DESC"))

        ### Pagination
        if "range" in request.args:
            start, end = json.loads(request.args["range"])
            total = len(out_l)
            end=min(end,total-1)
            out_l=out_l[start:end+1]
            headers = headers | content_range_headers("groups",start,end,total)

        return Response(json.dumps(out_l),content_type="application/json",headers=headers)

    def api_get_compute_latest(self,request,param=None):

        headers=STANDARD_HEADERS

        db_writer = cosmosdb.CosmosDBWriter()
        _ = db_writer.get_container("compute_latest","Accounting")

        if param:
            ### Respond to getOne (will be a little different from the usual filtered output)
            out={ 'id':param }
            compute_query = db_writer.query("compute_latest",where=[f"user = '{param}' OR c.project = '{param}'"])
            if compute_query:
                tmp = { i['user']:0.0 for i in compute_query if i['user'] not in ( 'grant','total' )} | { i['project']:0.0 for i in compute_query }
                for line in compute_query:
                    if line['user'] in ( 'grant','total' ): continue
                    tmp[line['user']] = round(line['usage']+tmp[line['user']],2)
                    tmp[line['project']] = round(line['usage']+tmp[line['project']],2)
            else:
                tmp = {}

            return Response(json.dumps(remove_internal_data_single(out | dict(sorted(tmp.items(), key=lambda x: x[1], reverse=True)))),content_type="application/json",headers=headers)

        where_list=[]

        if "filter" in request.args:
            filt=json.loads(request.args.get("filter"))
            for k,v in filt.items():
                ### Handle timestamps same as any other field in this case
                ### Should never filter this db on timestamps anyway
                #if k == "ts": continue
                if isinstance(v,List):
                    filt[k] = ','.join( [ "'" + i + "'" for i in v] )
                elif isinstance(v,str):
                    filt[k] = "'" + v + "'"
                where_list.append(f"{k} IN ({filt[k]})")

        order_str=None
        if "sort" in request.args:
            field,order = json.loads(request.args.get("sort"))
            order_str = field
            if order == "DESC":
                order_str = order_str + " DESC"

        compute_queries = db_writer.query("compute_latest",fields=None,where=where_list,order=order_str)

        ### Pagination
        if "range" in request.args:
            start, end = json.loads(request.args["range"])
            total = len(compute_queries)
            end=min(end,total-1)
            compute_queries=compute_queries[start:end+1]
            headers = headers | content_range_headers("users",start,end,total)

        return Response(json.dumps(remove_internal_data(compute_queries)),content_type="application/json",headers=headers)

    def api_get_compute(self,request,param=None):

        headers=STANDARD_HEADERS

        quarters=[]
        filt={}
        if "filter" in request.args:
            filt=json.loads(request.args.get("filter"))

        ### First up. figure out how many containers we need to grab
        if filt:
            if "ts" in filt:
                if isinstance(filt["ts"],str):
                    filt["ts"] = [ filt["ts"], ]
                q_set = set()
                for v in filt["ts"]:
                    q_set.add(sanitize_time(v).quarter())
                quarters = sorted(list(q_set))
            else:
                quarters = [ Datetime_with_quarter.now().quarter(), ]

        else:
            quarters = [ Datetime_with_quarter.now().quarter(), ]
            

        db_writer = cosmosdb.CosmosDBWriter()
        _ = db_writer.get_container("compute","Accounting",quarterly=True)
        
        if param:
            self.error_400()

        where_list=[]
        
        if filt:
            for k,v in filt.items():
                ### Handle timestamps later
                if k == "ts": continue
                if isinstance(v,List):
                    filt[k] = ','.join( [ "'" + i + "'" for i in v] )
                elif isinstance(v,str):
                    filt[k] = "'" + v + "'"
                where_list.append(f"{k} IN ({filt[k]})")
        
        order_str=None
        if "sort" in request.args:
            field,order = json.loads(request.args.get("sort"))
            order_str = field
            if order == "DESC":
                order_str = order_str + " DESC"
        
        start = None
        total = None
        if "range" in request.args:
            start, end = json.loads(request.args["range"])
            total=end-start+1
            headers = headers | content_range_headers("compute",start,end,total)

        compute_queries=[]
        for q in quarters:
            where_list_w_timestamps = where_list
            if filt:
                timestamps = filt.get("ts",None)
                if timestamps:
                    ### Compute data is six-hourly at 0, 6, 12 and 18 UTC
                    ### Though there is quite a bit of leeway there
                    ### If timestamps is a string, construct a between statement
                    ### for the hours either side of those
                    if isinstance(timestamps,str):
                        t=sanitize_time(timestamps)
                        where_list_w_timestamps = where_list + [ f"ts > '{(t - one_hour).isoformat()}'", f"ts < '{(t + one_hour).isoformat()}'" ]
                    elif isinstance(timestamps,List):
                        ### Handle an interval otherwise
                        timelist=sorted(timestamps)
                        tstart = sanitize_time(timelist[0])
                        tend = sanitize_time(timelist[-1])
                        where_list_w_timestamps = where_list + [ f"ts > '{(tstart - one_hour).isoformat()}'", f"ts < '{(tend + one_hour).isoformat()}'"]
            compute_queries.extend(db_writer.query("compute",fields=None,where=where_list_w_timestamps,order=order_str,offset=start,limit=total,quarter=q))

        return Response(json.dumps(remove_internal_data(compute_queries)),content_type="application/json",headers=headers)

    def api_get_storage_latest(self,request,param=None):

        headers=STANDARD_HEADERS

        db_writer = cosmosdb.CosmosDBWriter()
        _ = db_writer.get_container("files_report_latest","Accounting")
        _ = db_writer.get_container("storage_latest","Accounting")

        if param:
            ### Not necessary to support
            return self.error_400()

        where_list=[]
        quota_where_list=[]

        if "filter" in request.args:
            filt=json.loads(request.args.get("filter"))
            quota_filt={}
            for k,v in filt.items():
                ### Handle timestamps same as any other field in this case
                ### Should never filter this db on timestamps anyway
                #if k == "ts": continue
                if isinstance(v,List):
                    filt[k] = ','.join( [ "'" + i + "'" for i in v] )
                elif isinstance(v,str):
                    filt[k] = "'" + v + "'"
                where_list.append(f"{k} IN ({filt[k]})")
                if k in ( 'ownership', 'location' ):
                    quota_filt["project"] = filt[k]
                elif k in ( 'fs', 'system', 'PartitionKey' ):
                    quota_filt[k] = filt[k]
            quota_where_list=[ f"{k} IN ({v})" for k,v in quota_filt.items() ]

        order_str=None
        if "sort" in request.args:
            field,order = json.loads(request.args.get("sort"))
            order_str = field
            if order == "DESC":
                order_str = order_str + " DESC"

        ### Don't want totals if we're looking for a specific user
        quota_queries=[]
        storage_queries = db_writer.query("files_report_latest",fields=None,where=where_list,order=order_str)
        if "user" not in filt:
            totals_queries = db_writer.query("storage_latest",fields=None,where=quota_where_list,order=order_str)
            for i in totals_queries:
                quota_queries.append({"ts":i["ts"], 
                                        "id":f'{i["system"]}_{i["fs"]}_total_{i["project"]}_{i["project"]}',
                                        "fs":i["fs"],
                                        "user":"total",
                                        "ownership":i["project"],
                                        "location":i["project"],
                                        "size":i["usage"],
                                        "inodes":i["iusage"],
                                        })
                quota_queries.append({"ts":i["ts"], 
                                        "id":f'{i["system"]}_{i["fs"]}_quota_{i["project"]}_{i["project"]}',
                                        "fs":i["fs"],
                                        "user":"grant",
                                        "ownership":i["project"],
                                        "location":i["project"],
                                        "size":i["quota"],
                                        "inodes":i["iquota"],
                                        })

            storage_queries = storage_queries + quota_queries

            if order_str:
                reverseSort=False
                order_tuple = order_str.split(' ')
                if len(order_tuple) == 2:
                    reverseSort=(order_tuple[1] == "DESC")
                storage_queries = sorted(storage_queries,key=lambda x:x[order_tuple[0]],reverse=reverseSort)

        ### Pagination
        if "range" in request.args:
            start, end = json.loads(request.args["range"])
            total = len(storage_queries)
            end=min(end,total-1)
            storage_queries=storage_queries[start:end+1]
            headers = headers | content_range_headers("users",start,end,total)

        return Response(json.dumps(remove_internal_data(storage_queries)),content_type="application/json",headers=headers)

    def api_get_storage_project_latest(self,request,param=None):

        headers=STANDARD_HEADERS

        db_writer = cosmosdb.CosmosDBWriter()
        _ = db_writer.get_container("storage_latest","Accounting")

        if param:
            ### Don't need to support
            return self.error_400()

        where_list=[]

        if "filter" in request.args:
            filt=json.loads(request.args.get("filter"))
            for k,v in filt.items():
                ### Handle timestamps same as any other field in this case
                ### Should never filter this db on timestamps anyway
                #if k == "ts": continue
                if isinstance(v,List):
                    filt[k] = ','.join( [ "'" + i + "'" for i in v] )
                elif isinstance(v,str):
                    filt[k] = "'" + v + "'"
                where_list.append(f"{k} IN ({filt[k]})")

        order_str=None
        if "sort" in request.args:
            field,order = json.loads(request.args.get("sort"))
            order_str = field
            if order == "DESC":
                order_str = order_str + " DESC"

        storage_queries = db_writer.query("storage_latest",fields=None,where=where_list,order=order_str)

        ### Pagination
        if "range" in request.args:
            start, end = json.loads(request.args["range"])
            total = len(storage_queries)
            end=min(end,total-1)
            storage_queries=storage_queries[start:end+1]
            headers = headers | content_range_headers("users",start,end,total)

        return Response(json.dumps(remove_internal_data(storage_queries)),content_type="application/json",headers=headers)

    def api_get_storage(self,request,param=None):

        headers=STANDARD_HEADERS

        quarters=[]
        filt={}
        if "filter" in request.args:
            filt=json.loads(request.args.get("filter"))

        ### First up. figure out how many containers we need to grab
        if filt:
            if "ts" in filt:
                if isinstance(filt["ts"],str):
                    filt["ts"] = [ filt["ts"], ]
                q_set = set()
                for v in filt["ts"]:
                    q_set.add(sanitize_time(v).quarter())
                quarters = sorted(list(q_set))
            else:
                quarters = [ Datetime_with_quarter.now().quarter(), ]

        else:
            quarters = [ Datetime_with_quarter.now().quarter(), ]

        db_writer = cosmosdb.CosmosDBWriter()
        _ = db_writer.get_container("files_report","Accounting",quarterly=True)
        _ = db_writer.get_container("storage","Accounting",quarterly=True)

        if param:
            self.error_400()

        where_list=[]
        quota_where_list=[]

        if filt:
            quota_filt={}
            for k,v in filt.items():
                ### Handle timestamps later
                if k == "ts": continue
                if isinstance(v,List):
                    filt[k] = ','.join( [ "'" + i + "'" for i in v] )
                elif isinstance(v,str):
                    filt[k] = "'" + v + "'"
                where_list.append(f"{k} IN ({filt[k]})")
                if k in ( 'ownership', 'location' ):
                    quota_filt["project"] = filt[k]
                elif k in ( 'fs', 'system', 'PartitionKey' ):
                    quota_filt[k] = filt[k]
            quota_where_list=[ f"{k} IN ({v})" for k,v in quota_filt.items() ]

        order_str=None
        if "sort" in request.args:
            field,order = json.loads(request.args.get("sort"))
            order_str = field
            if order == "DESC":
                order_str = order_str + " DESC"

        start = None
        total = None
        if "range" in request.args:
            start, end = json.loads(request.args["range"])
            total=end-start+1
            headers = headers | content_range_headers("compute",start,end,total)

        do_total_query=False
        do_grant_query=False
        if "user" in filt:
            if any( [i == "'total'" for i in filt["user"].split(',')] ):
                do_total_query=True
            if any( [i == "'quota'" for i in filt["user"].split(',')] ):
                do_grant_query=True
        else:
            do_total_query=True
            do_grant_query=True

        quota_queries=[]
        storage_queries=[]
        for q in quarters:
            where_list_w_timestamps = where_list
            if filt:
                timestamps = filt.get("ts",None)
                if timestamps:
                    ### Quota data is daily at 2205 UTC
                    if isinstance(timestamps,str):
                        t=sanitize_time(timestamps)
                        where_list_w_timestamps = where_list + [ f"ts > '{(t - one_hour).isoformat()}'", f"ts < '{(t + one_hour).isoformat()}'" ]
                        quota_where_list_w_timestamps = quota_where_list + [ f"ts > '{(t - one_hour).isoformat()}'", f"ts < '{(t + one_hour).isoformat()}'" ]
                    elif isinstance(timestamps,List):
                        ### Handle an interval otherwise
                        timelist=sorted(timestamps)
                        tstart = sanitize_time(timelist[0])
                        tend = sanitize_time(timelist[-1])
                        where_list_w_timestamps = where_list + [ f"ts > '{(tstart - one_hour).isoformat()}'", f"ts < '{(tend + one_hour).isoformat()}'"]
                        quota_where_list_w_timestamps = quota_where_list + [ f"ts > '{(tstart - one_hour).isoformat()}'", f"ts < '{(tend + one_hour).isoformat()}'"]
            storage_queries.extend(db_writer.query("files_report",fields=None,where=where_list_w_timestamps,order=order_str,offset=start,limit=total,quarter=q))
            if do_grant_query | do_total_query:
                ### quota/usage, on the other hand comes in at 0, 6, 12 and 18 UTC from a different table
                totals_queries = db_writer.query("storage",fields=None,where=quota_where_list_w_timestamps,order=order_str,quarter=q)
                for i in totals_queries:
                    if do_total_query:
                        quota_queries.append({"ts":i["ts"],
                                            "id":f'{i["system"]}_{i["fs"]}_total_{i["project"]}_{i["project"]}',
                                            "fs":i["fs"],
                                            "user":"total",
                                            "ownership":i["project"],
                                            "location":i["project"],
                                            "size":i["usage"],
                                            "inodes":i["iusage"],
                                            })
                    if do_grant_query:
                        quota_queries.append({"ts":i["ts"],
                                            "id":f'{i["system"]}_{i["fs"]}_quota_{i["project"]}_{i["project"]}',
                                            "fs":i["fs"],
                                            "user":"grant",
                                            "ownership":i["project"],
                                            "location":i["project"],
                                            "size":i["quota"],
                                            "inodes":i["iquota"],
                                            })
        storage_queries = storage_queries + quota_queries

        if order_str:
            reverseSort=False
            order_tuple = order_str.split(' ')
            if len(order_tuple) == 2:
                reverseSort=(order_tuple[1] == "DESC")

            storage_queries = sorted(storage_queries,key=lambda x:x[order_tuple[0]],reverse=reverseSort)

        return Response(json.dumps(remove_internal_data(storage_queries)),content_type="application/json",headers=headers)

werkzeug_app = AccountingAPI({})

app = func.WsgiFunctionApp(app=werkzeug_app, 
                           http_auth_level=func.AuthLevel.ANONYMOUS)