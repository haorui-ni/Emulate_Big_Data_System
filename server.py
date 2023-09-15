import pymongo
from flask import Flask, jsonify, request
from bson.objectid import ObjectId
from pymongo.collection import ReturnDocument
import json
from bson import json_util
import random
from pymongo import ASCENDING, MongoClient
import secrets
from collections import OrderedDict


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["dsci551"]
app = Flask(__name__)

def custom_key(element):
    if isinstance(element, str):
        return element.lower()  # sort strings case-insensitively
    else:
        return element  # assume all non-string elements are integers and return them as is

# GET function
@app.route('/', defaults={'myPath': ''}, )
@app.route('/<path:myPath>', methods=['GET'])
def get_value(myPath):
    path = request.path
    url = path.split(".")[0]
    url_ls = url.split("/")[1:]
    try:
        collection = url_ls[0]
        col = db[collection]
    except:
        return jsonify({"error": "Could not resolve host"}), 200
    condition = {}
    result = []

    orderBy = request.args.get('orderBy')
    equalTo = request.args.get('equalTo')
    startAt = request.args.get('startAt')
    endAt = request.args.get('endAt')
    limitToFirst = request.args.get('limitToFirst')
    limitToLast = request.args.get('limitToLast')
    #orderBy = orderBy.replace('"', '')
    if len(url_ls) == 1:
        if orderBy:
            orderBy = orderBy.replace('"', '')
            condition[orderBy] = {}
            if equalTo:
                if equalTo.isdigit():
                    condition[orderBy]["$eq"] = int(equalTo)
                else:
                    condition[orderBy]["$eq"] = equalTo.replace('"', '')
            if startAt:
                if startAt.isdigit():
                    condition[orderBy]["$gte"] = int(startAt)
                else:
                    condition[orderBy]["$gte"] = startAt.replace('"', '')
            if endAt:
                if endAt.isdigit():
                    condition[orderBy]["$lte"] = int(endAt)
                else:
                    condition[orderBy]["$lte"] = endAt.replace('"', '')
            if not condition[orderBy]:
                condition = {}
            
            if orderBy == "$key":
                key_ls = []
                docs = col.find({}, {'_id': 0})
                for doc in docs:
                    keys = [key for key in doc.keys() if key != '_id']
                    for key in keys:
                        if key.isdigit():
                            key_ls.append(int(key))
                        else:
                            key_ls.append(key)
                key_ls.sort(key=lambda x: (isinstance(x, int), x) if isinstance(x, int) else (float('inf'), x))
                if equalTo:
                    doc_equal = {}
                    equalTo = equalTo.replace('"', '')
                    if isinstance(equalTo, int) or equalTo.isdigit():
                        equalTo = int(equalTo)
                    for key in key_ls:
                        if equalTo == key:
                            doc_equal = col.find_one({str(equalTo): {'$exists': True}}, {'_id': 0})
                            break
                    return jsonify(doc_equal)
            
                if startAt:
                    index = None
                    startAt = startAt.replace('"', '')
                    if isinstance(startAt, int) or startAt.isdigit():
                        startAt = int(startAt)
                        for key in key_ls:
                            if isinstance(key, int) or key.isdigit():
                                if int(key) >= startAt:
                                    index = key_ls.index(key)
                                    break
                            else:
                                index = key_ls.index(key)
                                break
                        if index:
                            key_ls = key_ls[index:]
                    else:
                            
                        for key in key_ls:
                            if startAt == key:
                                index = key_ls.index(key)
                                break
                        if index:
                            key_ls = key_ls[index:]

                if endAt:
                        index = None
                        endAt = endAt.replace('"', '')
                        if isinstance(endAt, int) or endAt.isdigit():
                            endAt = int(endAt)
                            for key in key_ls:
                                if isinstance(key, int) or key.isdigit():
                                    if int(key) > endAt:
                                        index = key_ls.index(key)
                                        break
                                else:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                key_ls = key_ls[:index]
                        else:
                            for key in key_ls:
                                if endAt == key:
                                    index = key_ls.index(key)
                                    break
                            if index and index < (len(key_ls)-1):
                                key_ls = key_ls[:index+1]
                if "-" in str(limitToFirst) or "-" in str(limitToLast):
                    return  jsonify({"error" : "limitToFirst/limitToLast must be positive"})

                if limitToFirst:
                    limitToFirst = limitToFirst.replace('"', '')
                    limitToFirst = int(limitToFirst)
                    key_ls = key_ls[:limitToFirst]
                if limitToLast:
                    limitToLast = limitToLast.replace('"', '')
                    limitToLast = int(limitToLast)          
                    i = len(key_ls)-int(limitToLast)
                    key_ls = key_ls[i:]
                if limitToFirst and limitToLast:
                    return jsonify({"error" : "Only one of limitToFirst and limitToLast may be specified"})
                for key in key_ls:
                    doc = col.find_one({str(key): {"$exists": True}}, {"_id": 0})
                    if doc:
                        result.append(doc)
                return jsonify(result)
            
            if orderBy == "$value":
                value_ls = []
                value_dict = {}
                docs = col.find({}, {'_id': 0})
                for doc in docs:
                    for key, value in doc.items():
                        value_dict[key] = value
                sorted_value = sorted(value_dict.items(), key=lambda x: (isinstance(x[1], int), x[1]) if isinstance(x[1], int) else (float('inf'), x[1]))
                value_ls = [tup[1] for tup in sorted_value]
                print(value_ls)
                for value in value_ls:
                    print(type(value))
                    if not isinstance(value, (int,str)):
                        return jsonify({'error': 'not support orderBy $value'})
                
                if equalTo:
                    doc_equal = {}
                    equalTo = equalTo.replace('"', '')
                    if isinstance(equalTo, int) or equalTo.isdigit():
                        equalTo = int(equalTo)
                    for key, value in sorted_value:
                        if equalTo == value:
                            doc_equal[key] = value
                            break
                    return jsonify(doc_equal), 200
                
                if startAt:
                    index = None
                    startAt = startAt.replace('"', '')
                    if isinstance(startAt, int) or startAt.isdigit():
                        startAt = int(startAt)
                        for value in value_ls:
                            if isinstance(value, int) or value.isdigit():
                                if int(value) >= startAt:
                                    index = value_ls.index(value)
                                    break
                            else:
                                index = value_ls.index(value)
                                break
                        if index:
                            sorted_value = sorted_value[index:]
                            print(sorted_value)
                    else:
                        for value in value_ls:
                            if startAt == value:
                                index = value_ls.index(value)
                                break
                        if index:
                            sorted_value = sorted_value[index:]
                if endAt:
                        value_ls = [tup[1] for tup in sorted_value]
                        index = None
                        endAt = endAt.replace('"', '')
                        if isinstance(endAt, int) or endAt.isdigit():
                            endAt = int(endAt)
                            for value in value_ls:
                                if isinstance(value, int) or value.isdigit():
                                    if int(value) > endAt:
                                        index = value_ls.index(value)
                                        break
                                else:
                                    index = value_ls.index(value)
                                    break
                            if index:
                                sorted_value = sorted_value[:index]
                        else:
                            for value in value_ls:
                                if endAt == value:
                                    index = value_ls.index(value)
                                    break
                            if index and index < (len(value_ls)-1):
                                sorted_value = sorted_value[:index+1]
                if limitToFirst:
                    limitToFirst = limitToFirst.replace('"', '')
                    limitToFirst = int(limitToFirst)
                    sorted_value = sorted_value[:limitToFirst]
                if limitToLast:
                    limitToLast = limitToLast.replace('"', '')
                    limitToLast = int(limitToLast)          
                    i = len(sorted_value)-int(limitToLast)
                    sorted_value = sorted_value[i:]
                if limitToFirst and limitToLast:
                    return jsonify({"error" : "Only one of limitToFirst and limitToLast may be specified"})
                sorted_value = [{pair[0]: pair[1]} for pair in sorted_value]
                return jsonify(sorted_value)
                
                    
            else:
                key_ls = []
                docs = col.find({},{'_id':0})
                for doc in docs:
                    keys = [key for key in doc.keys() if key != '_id']
                    for key in keys:
                        key = "$" + key + "." + orderBy
                        key_ls.append(key)
                pipeline = [
                    {
                        '$project': {
                            orderBy: {'$ifNull': key_ls},
                            'doc': '$$ROOT'
                            
                        }
                    },
                    { 
                        '$match': condition 
                    },
                    { 
                        '$sort': {orderBy: 1 } 
                    },
                    { 
                        '$replaceRoot': { 
                            'newRoot': '$doc' 
                        } 
                    }
                ]
                if "-" in str(limitToFirst) or "-" in str(limitToLast):
                    return  jsonify({"error" : "limit could not be negative"})
                if limitToFirst:
                    limitToFirst = int(limitToFirst)
                    pipeline.append({ '$limit': limitToFirst })
                pipeline.append({"$project": {"_id": 0}})
                for r in list(col.aggregate(pipeline)):
                    #r['_id'] = str(r['_id']) # Convert ObjectId to string
                    result.append(r)
                if limitToLast:
                    limitToLast = limitToLast.replace('"', '')
                    index = (len(result)-int(limitToLast))
                    result = result[index:]
                return jsonify(result)
        else:
            if equalTo or startAt or endAt or limitToFirst or limitToLast:
                return jsonify({"error" : "orderBy must be defined when other query parameters are defined"}), 404
            else:
                docs = col.find({}, {'_id': 0})
                for doc in docs:
                    result.append(doc)
                return jsonify(result), 200


    elif len(url_ls) == 2:
        k_id = url_ls[1]
        doc = col.find_one({str(k_id): {'$exists': True}}, {'_id': 0})
        if doc is None:
            return jsonify({"data": "%null"})
        if doc is not None:
            if orderBy:
                orderBy = orderBy.replace('"', '')
                if orderBy == "$key":
                    sorted_items = sorted(doc[str(k_id)].items(), key=lambda x: (isinstance(x, int), x) if isinstance(x, int) else (float('inf'), x))
                    print(sorted_items)
                    key_ls = [tup[0] for tup in sorted_items]
                    if equalTo:
                        doc= doc[str(k_id)]
                        doc_equal = {}
                        equalTo = equalTo.replace('"', '')
                        if isinstance(equalTo, int) or equalTo.isdigit():
                            equalTo = int(equalTo)
                        for key, value in doc.items():
                            if isinstance(key, int) or key.isdigit():
                                key = int(key)
                            if key == equalTo:
                                result = {}
                                result[key] = value
                                break
                        return jsonify(result), 200
                        
                    if startAt:
                        index = None
                        startAt = startAt.replace('"', '')
                        if isinstance(startAt, int) or startAt.isdigit():
                            startAt = int(startAt)
                            for key in key_ls:
                                if isinstance(key, int) or key.isdigit():
                                    if int(key) >= startAt:
                                        index = key_ls.index(key)
                                        break
                                else:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                sorted_items = sorted_items[index:]
                        else:
                                
                            for key in key_ls:
                                if startAt == key:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                sorted_items = sorted_items[index:]

                    if endAt:
                        key_ls = [tup[0] for tup in sorted_items]
                        index = None
                        endAt = endAt.replace('"', '')
                        if endAt.isdigit():
                            endAt = int(endAt)
                            for key in key_ls:
                                if isinstance(key, int) or key.isdigit():
                                    if int(key) > endAt:
                                        index = key_ls.index(key)
                                        break
                                else:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                sorted_items = sorted_items[:index]
                        else:
                            for key in key_ls:
                                if endAt == key:
                                    index = key_ls.index(key)
                                    break
                            if index and index < (len(key_ls)-1):
                                sorted_items = sorted_items[:index+1]

                    if "-" in str(limitToFirst) or "-" in str(limitToLast):
                        return  jsonify({"error" : "limit could not be negative"})
                    if limitToFirst:
                        limitToFirst = limitToFirst.replace('"', '')
                        limitToFirst = int(limitToFirst)
                        if limitToFirst>len(sorted_items):
                            return jsonify({"error" : "limit out of range"}), 404
                        try:
                            sorted_items = sorted_items[:limitToFirst]
                        except:
                            return jsonify({"error" : "limit out of range"}), 404
                    if limitToLast:
                        limitToLast = limitToLast.replace('"', '')
                        limitToLast = int(limitToLast)
                        if limitToLast>len(sorted_items):
                            return jsonify({"error" : "limit out of range"}), 404              
                        try:
                            i = len(sorted_items)-int(limitToLast)
                            sorted_items = sorted_items[i:]
                        except:
                            return jsonify({"error" : "limit out of range"}), 404
                    if limitToFirst and limitToLast:
                        return jsonify({"error" : "Only one of limitToFirst and limitToLast may be specified"}), 404
                    dict_data = {}
                    for item in sorted_items:
                        dict_data[item[0]] = item[1]
                    return jsonify(dict_data), 200
                    
                if orderBy == "$value":
                    sorted_items = sorted(doc[str(k_id)].items(), key=lambda x: (isinstance(x[1], int), x[1]) if isinstance(x[1], int) else (float('inf'), x[1]))
                    key_ls = [tup[1] for tup in sorted_items]
                    print(key_ls)
                    for key in key_ls:
                        if not isinstance(key, (int,str)):
                            return jsonify({'error': 'not support orderBy $value'})
                    if equalTo:
                        equalTo = equalTo.replace('"', '')
                        doc = doc[str(k_id)]
                        if isinstance(equalTo, int) or equalTo.isdigit():
                            equalTo = int(equalTo)
                        for key, value in doc.items():
                            if value == equalTo:
                                result = {}
                                result[key] = value
                                break
                        return jsonify(result), 200
                    if startAt:
                        index = None
                        startAt = startAt.replace('"', '')
                        if isinstance(startAt, int) or startAt.isdigit():
                            startAt = int(startAt)
                            for key in key_ls:
                                if isinstance(key, int) or key.isdigit():
                                    if int(key) >= startAt:
                                        index = key_ls.index(key)
                                        break
                                else:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                sorted_items = sorted_items[index:]
                        else:
                            for key in key_ls:
                                if startAt == key:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                sorted_items = sorted_items[index:]
                    if endAt:
                        key_ls = [tup[1] for tup in sorted_items]
                        index = None
                        endAt = endAt.replace('"', '')
                        if isinstance(endAt, int) or endAt.isdigit():
                            endAt = int(endAt)
                            for key in key_ls:
                                if isinstance(key, int) or key.isdigit():
                                    if int(key) > endAt:
                                        index = key_ls.index(key)
                                        break
                                else:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                sorted_items = sorted_items[:index]
                        else:
                            for key in key_ls:
                                if endAt == key:
                                    index = key_ls.index(key)
                                    break
                            if index and index < (len(key_ls)-1):
                                sorted_items = sorted_items[:index+1]

                    if "-" in str(limitToFirst) or "-" in str(limitToLast):
                        return  jsonify({"error" : "limit could not be negative"})
                    if limitToFirst:
                        limitToFirst = limitToFirst.replace('"', '')
                        limitToFirst = int(limitToFirst)
                        if limitToFirst>len(sorted_items):
                            return jsonify({"error" : "limit out of range"}), 404
                        try:
                            sorted_items = sorted_items[:limitToFirst]
                        except:
                            return jsonify({"error" : "limit out of range"}), 404
                    if limitToLast:
                        limitToLast = limitToLast.replace('"', '')
                        limitToLast = int(limitToLast)
                        if limitToLast>len(sorted_items):
                            return jsonify({"error" : "limit out of range"}), 404              
                        try:
                            i = len(sorted_items)-int(limitToLast)
                            sorted_items = sorted_items[i:]
                        except:
                            return jsonify({"error" : "limit out of range"}), 404
                    if limitToFirst and limitToLast:
                        return jsonify({"error" : "Only one of limitToFirst and limitToLast may be specified"}), 404
                    sorted_items = [{pair[0]: pair[1]} for pair in sorted_items]
                    return jsonify(sorted_items), 200
                else:
                    dict_data = {}
                    for item in sorted_items:
                        dict_data[item[0]] = item[1]
                    return jsonify(dict_data), 200
        result = col.find_one({str(k_id): {'$exists': True}}, {'_id':0})
        return jsonify(result[k_id])

            

    else:
        k_id = url_ls[1]
        key_ls = url_ls[2:]
        doc = col.find_one({str(k_id): {'$exists': True}}, {'_id': 0})
        if doc is None:
            return jsonify({"data": "null"})
        doc = doc[str(k_id)]
        for key in key_ls:
            if key in doc:
                doc = doc[key]
            else:
                doc = None
                break
        if doc is not None:
            if orderBy:
                orderBy = orderBy.replace('"', '')

                if orderBy == "$key":
                    sorted_items = sorted(doc.items(), key=lambda x: (isinstance(x, int), x) if isinstance(x, int) else (float('inf'), x))
                    key_ls = [tup[0] for tup in sorted_items]
                    if equalTo:
                        equalTo = equalTo.replace('"', '')
                        if isinstance(equalTo, int) or equalTo.isdigit():
                            equalTo = int(equalTo)

                        for key, value in doc.items():
                            if isinstance(key, int) or key.isdigit():
                                key = int(key)
                            if key == equalTo:
                                result = {}
                                result[key] = value
                                break
                        return jsonify(result), 200
                    if startAt:
                        index = None
                        startAt = startAt.replace('"', '')
                        if isinstance(startAt, int) or startAt.isdigit():
                            startAt = int(startAt)
                            for key in key_ls:
                                if isinstance(key, int) or key.isdigit():
                                    if int(key) >= startAt:
                                        index = key_ls.index(key)
                                        break
                                else:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                sorted_items = sorted_items[index:]
                        else:
                            
                            for key in key_ls:
                                if startAt == key:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                sorted_items = sorted_items[index:]

                    if endAt:
                        key_ls = [tup[0] for tup in sorted_items]
                        index = None
                        endAt = endAt.replace('"', '')
                        if endAt.isdigit():
                            endAt = int(endAt)
                            for key in key_ls:
                                if isinstance(key, int) or key.isdigit():
                                    if int(key) > endAt:
                                        index = key_ls.index(key)
                                        break
                                else:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                sorted_items = sorted_items[:index]
                        else:
                            for key in key_ls:
                                if endAt == key:
                                    index = key_ls.index(key)
                                    break
                            if index and index < (len(key_ls)-1):
                                sorted_items = sorted_items[:index+1]
                        
                    if "-" in str(limitToFirst) or "-" in str(limitToLast):
                        return  jsonify({"error" : "limit could not be negative"})
                    if limitToFirst:
                        limitToFirst = limitToFirst.replace('"', '')
                        limitToFirst = int(limitToFirst)
                        if limitToFirst>len(sorted_items):
                            return jsonify({"error" : "limit out of range"}), 404
                        try:
                            sorted_items = sorted_items[:limitToFirst]
                        except:
                            return jsonify({"error" : "limit out of range"}), 404
                    if limitToLast:
                        limitToLast = limitToLast.replace('"', '')
                        limitToLast = int(limitToLast)
                        if limitToLast>len(sorted_items):
                            return jsonify({"error" : "limit out of range"}), 404             
                        try:
                            i = len(sorted_items)-int(limitToLast)
                            sorted_items = sorted_items[i:]
                        except:
                            return jsonify({"error" : "limit out of range"}), 404
                    if limitToFirst and limitToLast:
                        return jsonify({"error" : "Only one of limitToFirst and limitToLast may be specified"}), 404
                    dict_data = {}
                    for item in sorted_items:
                        dict_data[item[0]] = item[1]
                    return jsonify(dict_data), 200
                
                if orderBy == "$value":
                    sorted_items = sorted(doc.items(), key=lambda x: (isinstance(x[1], int), x[1]) if isinstance(x[1], int) else (float('inf'), x[1]))
                    key_ls = [tup[1] for tup in sorted_items]
                    for key in key_ls:
                        if not isinstance(key, (int,str)):
                            return jsonify({'error': 'not support orderBy $value'})
                    if equalTo:
                        equalTo = equalTo.replace('"', '')
                        if isinstance(equalTo, int) or equalTo.isdigit():
                            equalTo = int(equalTo)
                        for key, value in doc.items():
                            if value == equalTo:
                                result = {}
                                result[key] = value
                                break
                        return jsonify(result), 200
                    if startAt:
                        index = None
                        startAt = startAt.replace('"', '')
                        if isinstance(startAt, int) or startAt.isdigit():
                            startAt = int(startAt)
                            for key in key_ls:
                                if isinstance(key, int) or key.isdigit():
                                    if int(key) >= startAt:
                                        index = key_ls.index(key)
                                        break
                                else:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                sorted_items = sorted_items[index:]
                        else:
                            for key in key_ls:
                                if startAt == key:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                sorted_items = sorted_items[index:]

                    if endAt:
                        key_ls = [tup[1] for tup in sorted_items]
                        index = None
                        endAt = endAt.replace('"', '')
                        if isinstance(endAt, int) or endAt.isdigit():
                            endAt = int(endAt)
                            for key in key_ls:
                                print(key)
                                if isinstance(key,int) or key.isdigit():
                                    if int(key) > endAt:
                                        index = key_ls.index(key)
                                        print(index)
                                        print(key)
                                        break
                                else:
                                    index = key_ls.index(key)
                                    break
                            if index:
                                print(index)
                                sorted_items = sorted_items[:index]
                        else:
                            for key in key_ls:
                                if endAt == key:
                                    index = key_ls.index(key)
                                    break
                            if index and index < (len(key_ls)-1):
                                sorted_items = sorted_items[:index+1]

                    if "-" in str(limitToFirst) or "-" in str(limitToLast):
                        return  jsonify({"error" : "limit could not be negative"})
                    if limitToFirst:
                        limitToFirst = limitToFirst.replace('"', '')
                        limitToFirst = int(limitToFirst)
                        if limitToFirst>len(sorted_items):
                            return jsonify({"error" : "limit out of range"}), 404
                        try:
                            sorted_items = sorted_items[:limitToFirst]
                        except:
                            return jsonify({"error" : "limit out of range"}), 404
                    if limitToLast:
                        limitToLast = limitToLast.replace('"', '')
                        limitToLast = int(limitToLast)
                        if limitToLast>len(sorted_items):
                            return jsonify({"error" : "limit out of range"}), 404             
                        try:
                            i = len(sorted_items)-int(limitToLast)
                            sorted_items = sorted_items[i:]
                        except:
                            return jsonify({"error" : "limit out of range"}), 404
                    if limitToFirst and limitToLast:
                        return jsonify({"error" : "Only one of limitToFirst and limitToLast may be specified"}), 404
                    
                    ordered_dict_data = OrderedDict(sorted_items)
                    ordered_data = [{k: v} for k, v in ordered_dict_data.items()]
                    return jsonify(ordered_data), 200
                
            else:
                response_obj = {str(url_ls[-1]): doc}
                return jsonify(response_obj), 200
        response_obj = {str(url_ls[-1]): doc}
        return jsonify(response_obj[str(url_ls[-1])]), 404

# PUT function
@app.route('/', defaults={'myPath': ''}, )
@app.route('/<path:myPath>', methods=['PUT'])
def put_value(myPath):
    path = request.path
    data = request.get_data().decode('utf-8')
    # if data like '"string"', overwrite with {"data": ""}
    if isinstance(data, str) and not any(char in data for char in ['{', '}', ':']):
        data = data.replace("\"", "").replace("\\", "")
        data = {data: ""}
    else:
        try:
            data = json.loads(data)
        except:
            return jsonify({"error" : "Invalid data; couldn't parse JSON object, array, or value."}), 404
    #data_keys = list(data.keys())
    #data_values = list(data.values())
    url = path.split(".")[0]
    url_ls = url.split("/")[1:]
    if len(url_ls) == 0:
        return jsonify({"error": "Could not resolve host"}), 404
    col = url_ls[0]
    col = db[col]
    if len(url_ls) == 1:
        print(data)
        col.delete_many({})
        col.insert_one(data)
        data = list(col.find({}, {'_id': 0}))
        return jsonify(data), 200
    elif len(url_ls) == 2:  # if id not exists insert, if exists overwirite the doc
        k_id = url_ls[1]
        data = {k_id: data}
        col.find_one_and_replace({str(k_id): {'$exists': True}}, data, upsert=True)
        new_doc = col.find_one({str(k_id): {'$exists': True}}, {'_id':0})
        return jsonify(new_doc[k_id])
        #return jsonify({'message': 'Data updated/inserted successfully'}), 200
    else:
        k_id = url_ls[1]
        nested_keys = url_ls[2:-1]
        last_key = url_ls[-1]
        existing_doc = col.find_one({k_id: {'$exists': True}})
        existing_doc = existing_doc[k_id] if existing_doc else {}
        temp_doc = existing_doc
        for key in nested_keys:
            if key in temp_doc:
                temp_doc = temp_doc[key]
            else:
                return jsonify({'error': '%null'}), 404
        if last_key in temp_doc:
            if isinstance(temp_doc[last_key], dict) and isinstance(data, dict):
                temp_doc[last_key] = data
            else:
                temp_doc[last_key] = data
        else:
            temp_doc[last_key] = data
        col.find_one_and_replace(
            {str(k_id): {'$exists': True}},
            {k_id: existing_doc},
            upsert=True
        )
        return jsonify(data), 200

# PATCH
app.route('/', defaults={'myPath': ''}, )
@app.route('/<path:myPath>', methods=['PATCH'])
def patch_value(myPath):
    path = request.path
    data = request.get_data().decode('utf-8')
    try:
        data = json.loads(data)
    except:
        return jsonify({"error" : "Invalid data; couldn't parse JSON object, array, or value."}), 404
    url = path.split(".")[0]
    url_ls = url.split("/")[1:]
    if len(url_ls) == 0:
        return jsonify({"error": "Could not resolve host"}), 404
    col = url_ls[0]
    col = db[col]
    if len(url_ls) == 1: 
        for k, v in data.items():
            result = col.update_one({k: {'$exists': True}}, {'$set': {k: v}}, upsert=True)
        return jsonify(data), 200
    elif len(url_ls) == 2: # id exists update doc, not exists insert new doc
        k_id = url_ls[1]
        existing_doc = col.find_one({k_id: {'$exists': True}})
        if existing_doc:
            existing_doc = existing_doc.get(k_id, {})
            update_doc = {f"{k_id}.{k}": v for k, v in data.items()}
            result = col.update_one({k_id: {'$exists': True}}, {'$set': update_doc}, upsert=True)
            new_doc = col.find({str(k_id): {'$exists': True}}, {'_id':0})
            return jsonify(data)
            #return jsonify({'message': 'document updated successfully'}), 200
        else:
            col.insert_one({k_id: data})
            new_doc = col.find({str(k_id): {'$exists': True}}, {'_id':0})
            return jsonify(data)
            #return jsonify({'message': 'document inserted successfully'}), 201
    else:
        k_id = url_ls[1]
        nested_keys = url_ls[2:-1]
        last_key = url_ls[-1]
        existing_doc = col.find_one({str(k_id): {'$exists': True}})
        existing_doc = existing_doc[k_id]
        if existing_doc:
            update_doc = existing_doc
        else:
            update_doc = {}
        temp_doc = update_doc
        for key in nested_keys:
            temp_doc = temp_doc.setdefault(key, {})
        if last_key in temp_doc:
            if isinstance(temp_doc[last_key], dict) and isinstance(data, dict):
                temp_doc[last_key].update(data)
            else:
                temp_doc[last_key] = data
        else:
            temp_doc[last_key] = data
        result = col.update_one({str(k_id): {'$exists': True}}, {'$set': {k_id: update_doc}}, upsert=True)
        return jsonify(data), 200

# POST
app.route('/', defaults={'myPath': ''}, )
@app.route('/<path:myPath>', methods=['POST'])
def post_value(myPath):
    path = request.path
    data = request.get_data().decode('utf-8')
    try:
        data = json.loads(data)
    except:
        return jsonify({"error" : "Invalid data; couldn't parse JSON object, array, or value."}), 404
    url = path.split(".")[0]
    url_ls = url.split("/")[1:]
    if len(url_ls) == 0:
        return jsonify({"error": "Could not resolve host"}), 404
    col = url_ls[0]
    col = db[col]
    if len(url_ls) == 1:
        random_key = key = secrets.token_urlsafe(20)
        new_doc = {
                random_key: data
            }
        result = col.insert_one(new_doc)
        return jsonify({random_key:data})
        #return jsonify({'message': 'document updated successfully'}), 200
    elif len(url_ls) == 2:
        k_id = url_ls[1]
        document = col.find_one({k_id: {'$exists': True}})
        if document:
            random_key = secrets.token_urlsafe(20)
            col.update_one({k_id: {'$exists': True}}, {'$set':{f'{k_id}.{random_key}': data}}, upsert=True)
            return jsonify([{random_key:data}])
        else:
            random_key = secrets.token_urlsafe(20)
            new_doc = {k_id: {
                random_key: data
            }}
            col.insert_one(new_doc)
            return jsonify({random_key:data})

        #return jsonify({"message": "Document inserted successfully."}), 201
    else:
        k_id = url_ls[1]
        nested_keys = url_ls[2:-1]
        last_key = url_ls[-1]
        existing_doc = col.find_one({str(k_id): {'$exists': True}})
        if existing_doc:
            update_doc = existing_doc[k_id]
        else:
            update_doc = {}
        temp_doc = update_doc
        for key in nested_keys:
            temp_doc = temp_doc.setdefault(key, {})
        random_key = secrets.token_urlsafe(20)
        if last_key in temp_doc:
            if isinstance(temp_doc[last_key], dict) and isinstance(data, dict):
                temp_doc[last_key].update({random_key: data})
            else:
                temp_doc[last_key] = {random_key: data}
        else:
            temp_doc[last_key] = {random_key: data}
        result = col.update_one({str(k_id): {'$exists': True}}, {'$set': {k_id: update_doc}}, upsert=True)
        return jsonify({random_key:data}), 200

# DELETE
app.route('/', defaults={'myPath': ''}, )
@app.route('/<path:myPath>', methods=['DELETE'])
def delete_value(myPath):
    path = request.path
    print(path, type(path))
    url = path.split(".")[0]
    url_ls = url.split("/")[1:]
    if len(url_ls) == 0:
        return jsonify({"error": "Could not resolve host"}), 404
    col = url_ls[0]
    col = db[col]
    if len(url_ls) == 1: # delete all docs
        try:
            col.delete_many({})
            return jsonify({'message': 'delete successfully'}), 200
        except:
            return jsonify({'message': 'error'}), 404
    elif len(url_ls) == 2:
        k_id = url_ls[1]
        result = col.delete_one({str(k_id): {'$exists': True}})
        return jsonify({'message': 'deleted successfully'})
    else:
        k_id = url_ls[1]
        nested_keys = url_ls[2:-1]
        last_key = url_ls[-1]
        existing_doc = col.find_one({str(k_id): {'$exists': True}})
        if existing_doc:
            update_doc = existing_doc[k_id]
        else:
            return jsonify({'message': 'error'}), 404
        temp_doc = update_doc
        for key in nested_keys:
            if key in temp_doc:
                temp_doc = temp_doc[key]
            else:
                return jsonify({'message': 'error'}), 404
        if last_key in temp_doc:
            del temp_doc[last_key]
            result = col.replace_one({str(k_id): {'$exists': True}}, {k_id: update_doc})
            return jsonify({'message': 'deleted successfully'}), 200
        else:
            return jsonify({'message': 'error'}), 404



app.run(debug=True)
