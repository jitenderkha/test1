import json
import urll1
from elasticsearch import Elasticsearch
import hash
import data
import regex

#In [2]: es_conn = Elasticsearch('localhost:9200')

#In [3]: query = {}

#In [4]: response = es_conn.search(index= 'test', doc_type= 'test', body = query)


#{"query":{"match":{"f1":"abc edf"}},"size": 100, "_source": ["name","id"]}

term='Dividends received from equity investee'


#url='http://localhost:9200/es/parsetab/_search?q=object.text:loss%20from%20operations & operator=AND'
#req=urllib2.Request(url)
#out=urllib2.urlopen(req)
q={
    "query" : {
        "nested" : {
            "path" : "",
            "query" : {
                "match" : {"object.text" : term},
#"inner_hits" : {}
            }
            #"inner_hits" : {}
        }
    }
}
"""
q={
    "query" : {
        "has_child" : {
            "type" : "",
            "query" : {
                "match" : {"text" : term}
            },
            "inner_hits" : {}
        }
    }
}"""

q={
"query":{
"bool": {
    "must": [
      { "match": { "docid": "yhoo1"}},
        {"nested" : {
            "path" : "object",
            "query" : {
                "prefix" : { "text":'d'}
            },
            "inner_hits" : {}
        }}

    ],

  }
  }}
q={
    "query" : {
        "nested" : {
            "path" : "object",
            "query" : {
                "prefix" : { "object.text":'Di'}
            },
            "inner_hits" : {}
        }
    }
}

q={
"query":{
"bool": {
    "must": [
      { "match": { "doc_id": "yhoo1"}},
      { "prefix": { "table_id": "8"}}

    ],

  }
  }}

q={
    "query" : {
        "nested" : {
            "path" : "object",
            "query" : {
                "match" : {"object.text" : "text:'Gross search revenue'"}
            },
            "inner_hits" : {}
        }
    }
}

#working
q={
    "query" : {
        "nested" : {
            "path" : "object",
            "query" : {
                "prefix" : {"object.text" : "d"}
            },
            "inner_hits" : {}
        }
    }
}

#working
q={
"query":{
"bool": {
    "must": [
      { "match": { "doc_id": "yhoo4"}},
        {"nested" : {
            "path" : "object",
            "query" : {
                "prefix" : { "object.text":'d'}
            },
            "inner_hits" : {}
        }}

    ],

  }
  }}
#gross mobile revenue
q={
    "query" : {
        "nested" : {
            "path" : "object",
            "query" : {
                "prefix" : { "object.text":'gross'}



            },
            "inner_hits" : {}
        }
#"operator": "and"
    }
}

t='cash an'
#final query
q1={
"query":{
"bool": {
    "must": [
      { "term": { "doc_id": "yhoo4"}},
        {"nested" : {
            "path" : "object",
            "query" : {
                "match_phrase_prefix" : { "object.text":t}

            },

            "inner_hits" : {}
        }}

    ]

  }
  },
    "_source" : False
}
#final_testing
"""
t1=["income taxes","payable"]
q={
"query":{
"bool": {
    "must": [
      { "term": { "doc_id": "yhoo4"}},
        {"nested" : {
            "path" : "object",
            "query" : {
                "terms" : { "object.text":t1}

            },

            "inner_hits" : {"_source":"text"}
        }}

    ]

  }
  },
    "_source" : False
}

"""

q={
    "query" : {
        "nested" : {
            "path" : "object",
            "query" : {
                "bool" : {
                    "should" :{

                            "match" : { "object.text":{"query":"cash ","operator":"and"}}

                        }}



            },
            "inner_hits" : {"_source":"text"}
        }
#"operator": "and"
    },
    "sort":[{"table_id":{"order":"asc"}},{"doc_id":{"order":"asc"}}],
    "_source": ['doc_id','table_id']

}

q={
    "query" : {
        "nested" : {
            "path" : "object",
            "query" : {
                "bool" : {
                    "should" :{

                            "match" : { "object.text":{"query":"cash ","operator":"and"}}

                        }}



            },
            "inner_hits" : {"_source":"text"}
        }
#"operator": "and"
    },
    #"sort":[{"object.text":{"order":"asc"}}],
    "_source": ['doc_id','table_id']

}


n_q={
    "query" : {
        "nested" : {
            "path" : "note_highlights",
            "query" : {
                "match_all":{}



            },
            "inner_hits" : {"_source":["ht_timestamp","updated_at"]}
        }
#"operator": "and"
    },
    "sort": [{"note_highlights.updated_at": {"order": "desc","nested_path":"note_highlights"}},{'note_updated_date':{"order":"desc"}}],
    "_source": False

}

#f1
q={
    "query" : {
        "bool":{
            "must":[
                {"term":{"username":"jitender"}},
            {"nested" : {
            "path" : "note_highlights",
            "query" : {
                "match_all":{}



            },
            "inner_hits" : {"_source":["ht_timestamp","updated_at","text"]}
        }}]
        }

    },
    "sort": [{"note_highlights.updated_at": {"order": "desc","mode":"max","nested_path":"note_highlights","nested_filter":{"match_all":{}}}}],
    "_source": False

}


#f2-final
q={
    "query" : {
        "bool":{
            "must":[
                {"term":{"username":"jitender"}},
            {"nested" : {
            "path" : "note_highlights",
            "query" : {
                "match_all":{}



            },
            "inner_hits" : {"_source":["ht_timestamp","updated_at","text"],
                            "sort": [{"note_highlights.updated_at": {"order": "desc"}}],
                            "size":15
                            }
        }}]
        }

    },
    "sort": [{"note_updated_date": {"order": "desc"}},{"note_highlights.updated_at": {"order": "desc","nested_path":"note_highlights","nested_filter":{"match_all":{}}}}],
    "_source": False

}

#ff-final
q={
    "query" : {
        "bool":{
            "must":[

            {"nested" : {
            "path" : "note_highlights",
            "query" : {
                "match_all":{}



            },
            "inner_hits" : {"_source":["ht_timestamp","updated_at"],
                            "sort": [{"note_highlights.updated_at": {"order": "desc"}}],
                            "size":15
                            }
        }}]
        }

    },
    "sort": [{"note_updated_date": {"order": "desc"}}],
    "_source": False

}

q={
    "query" : {

        "term":{"username":"jitender"}


    },
    "sort": [{"note_highlights.updated_at": {"order": "desc"}}],
    "_source": False

}



n_q={
    "query" : {
        "bool":{
            "must":[
                {"term":{"username":"jitender"}},
            {"nested" : {
            "path" : "note_highlights",
            "query" : {
                "match_all":{}



            },
            "inner_hits" : {"_source":["ht_timestamp","updated_at","text"]}
        }}]
        }
#"operator": "and"
    },
    "sort": [{"note_highlights.updated_at": {"order": "desc","nested_path":"note_highlights","nested_filter":{"match_all":{}}}}],
    "_source": False

}




t='revenue d'
q1={
"query":{
"bool": {
    "must": [
      { "term": { "usercontact": "333"}},
        {"nested" : {
            "path" : "note_highlights",
            "query" : {
                "match_phrase_prefix" : { "note_highlights.value":27}

            },"inner_hits" : {}}},
        {"nested" : {
            "path" : "note_comments",
            "query" : {
                "match_phrase_prefix" : { "note_comments.text":'net s'}

            },

            "inner_hits" : {}
        }}

    ]

  }
  },
    "_source" : ['field2','field3','field4']
}


#q={"query":{"match_all":{}},"_source":False}
#q={"query":{"match":{"object.text":term}},"size": 100, "_source": ["doc_id","table_id","object.value"]}
#q={"query":{"match":{"doc_id":'yhoo1'}}}
#q={"query":{"match_phrase":{"field2":"welcome here again"}},"_source":False}
#q={"query":{"match_phrase":{"field2":{"query":"welcome here","slop":2}}}}
#q={"query":{"match_phrase":{"field1":{"query":"total","slop":2}}}}
#q={"query":{"
aggs={"field4":{"terms":{"field":"field4.fld1","size":100}}}
h_q={"fields":{"field1":{"fragment_size" : 150, "number_of_fragments" : 100}}}
qr={"aggs":aggs,"highlight":h_q}
es_conn = Elasticsearch('localhost:9200')
from elastmap1 import *
# res=es_conn.index(index="els1",doc_type="doc1",body=res)
#res=es_conn.indices.create(index='els1',body=mapping,ignore=400)
#res=es_conn.indices.delete(index='els1',ignore=400)
print res
#qr= {"userContact":'111'}
action = [{
        '_index': 'els1',
        '_type': 'doc1',
        '_id': 'AVnPSwpYDFd-9uXSo-ir',
        '_op_type': 'update',
        'doc': qr
        }]
q={"query":{"match_all":{}},'_source':False}
#bulk_updated = helpers.bulk(es_conn, action,request_timeout=300)
#response = es_conn.index(index= 'els1', doc_type= 'doc1', body = res)
response = es_conn.search(index= 'els1', doc_type= 'doc1',from_=4,size=5)
print response
print '123123'
data=response
data=response.read()
data=json.loads(data)
print data
#print json.dumps(data)
# print 'total matched', data['hits']['total']
# t=data['hits']['total']

# for hit in data.get('hits').get('hits'):
#     inner_hits = hit.get('inner_hits').get('object')
#     for inner_hit in inner_hits.get('hits').get('hits'):
#         #print inner_hit.get('_source').get('text')
#         print inner_hit
#for i in range(t):
    #print 'match:', i+1, 'doc_id:', data['hits']['hits'][i]['_source']['doc_id'] ,'table_id:', data['hits']['hits'][i]['_source']['table_id']
    #print data['hits']['hits'][i]['_source']['doc_id']
    #print data['hits']['hits'][i]['_source']['table_id']
