# function_app.py

from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException, NotFound
from clex_functional_accounting.lib import cosmosdb,blob,group_list
import json
from datetime import datetime, timedelta

import azure.functions as func
from typing import Any, Dict, List, Tuple, Optional, Union

one_hour=timedelta(hours=1)

class Datetime_with_quarter(datetime):
    def quarter(self):
        return f"{self.year}.q{(self.month-1)//3+1}"

def content_range_headers(who: str, start: int, end: int, total: int):
    return {
        'Content-Range': f"{who} {start}-{end}/{total}",
        'Access-Control-Expose-Headers': 'Content-Range'
    }

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
            if k in [ "uid", "gid" ]: v = int(v)
            if isinstance(v,str) or isinstance(v,int): real_filt[k] = [v,]
        for l in in_list:
            if all( l[k] in v for k,v in real_filt.items() ):
                out_list.append(l)
        return out_list
    else:
        return in_list


class AccountingAPI(object):
    def __init__(self,config):

        url_map = []
        for func in dir(self):
            if func.startswith("api_get_"):
                url_map.append(Rule(f'/api/v0/{func.split("_")[-1]}',endpoint=func))
                url_map.append(Rule(f'/api/v0/{func.split("_")[-1]}/<param>',endpoint=func))
        
        self.url_map = Map(url_map)

    def error_404(self):
        return Response("{'data': 'Not Found'}",404,content_type="application/json")

    def error_400(self):
        return Response("{'data': 'Invalid'}",404,content_type="application/json")

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

        if param:
            try:
                return Response(json.dumps({"id":param} | user_d[param]),content_type="application/json")
            except KeyError:
                return Response('{}',content_type="application/json")

        all_user_list=[ {"id":k} | v for k,v in user_d.items() ]
        out_l=all_user_list
        extra_headers={}

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
            out_l=out_l[start:end]
            extra_headers = content_range_headers("users",start,end,total)

        return Response(json.dumps(out_l),content_type="application/json",headers=extra_headers)
    
    def api_get_groups(self,request: Request,param=None):

        blob_writer = blob.BlobWriter()
        group_d = blob_writer.read_item(blob.CONTAINER,'groups')

        if param:
            try:
                return Response(json.dumps({"id":param} | group_d[param]),content_type="application/json")
            except KeyError:
                return Response('{}',content_type="application/json")

        all_group_list=[ {"id":k} | v for k,v in group_d.items() ]
        out_l=all_group_list
        extra_headers={}

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
            out_l=out_l[start:end]
            extra_headers = content_range_headers("groups",start,end,total)

        return Response(json.dumps(out_l),content_type="application/json",headers=extra_headers)

    def api_get_compute(self,request,param=None):

        extra_headers={}

        quarters=[]
        ### First up. figure out how many containers we need to grab
        if "filter" in request.args:
            filt=json.loads(request.args.get("filter"))
            if "ts" in filt:
                if isinstance(filt["ts"],str):
                    filt["ts"] = [ filt["ts"], ]
                q_set = set()
                for v in filt["ts"]:
                    q_set.add(Datetime_with_quarter.fromisoformat(v).quarter())
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
        if "filter" in request.args:
            filt=json.loads(request.args.get("filter"))
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
            extra_headers = content_range_headers("groups",start,end,total)

        compute_queries=[]
        for q in quarters:
            timestamps = filt.get("ts",None)
            if timestamps:
                ### Compute data is six-hourly at 0, 6, 12 and 18 UTC
                ### Though there is quite a bit of leeway there
                ### If timestamps is a string, construct a between statement
                ### for the hours either side of those
                if isinstance(timestamps,str):
                    t=datetime.fromisoformat(timestamps)
                    where_list_w_timestamps = where_list + [ f"ts > '{(t - one_hour).isoformat()}'", f"ts < '{(t + one_hour).isoformat()}'" ]
                elif isinstance(timestamps,List):
                    ### Handle an interval otherwise
                    timelist=sorted(timestamps)
                    tstart = datetime.fromisoformat(timelist[0])
                    tend = datetime.fromisoformat(timelist[-1])
                    where_list_w_timestamps = where_list + [ f"ts > '{(tstart - one_hour).isoformat()}'", f"ts < '{(tend + one_hour).isoformat()}'"]
            else:
                where_list_w_timestamps = where_list
            compute_queries.extend(db_writer.query("compute",fields=None,where=where_list_w_timestamps,order=order_str,offset=start,limit=total,quarter=q))

        return Response(json.dumps(compute_queries),content_type="application/json",headers=extra_headers)

werkzeug_app = AccountingAPI({})

app = func.WsgiFunctionApp(app=werkzeug_app, 
                           http_auth_level=func.AuthLevel.ANONYMOUS)