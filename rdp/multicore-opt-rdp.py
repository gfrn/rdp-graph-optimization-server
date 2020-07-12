from flask import Flask, jsonify, request, render_template, make_response
import time # Usado apenas para debug
import requests
import json 
import math
import numpy as np
import multiprocessing as mp
from shapely.geometry import LineString 

# Faz a maior parte das operações em C; diferença para implementação de Visvalingamwyatt

app = Flask(__name__)

def simplify_rdp(proc, return_pts, tolerance, input_array):
        ls = LineString(input_array)
        return_pts[proc] = np.array((ls.simplify(tolerance, preserve_topology=False)))

def opt_rdp(q, raw_arr, tolerance):
    manager = mp.Manager()
    return_pts = manager.dict()

    lRaw = len(raw_arr)

    index = 0
    step = lRaw//q

    procList = []
    for i in range(0, q): # Divide a lista em fragmentos menores para serem computados paralelamente
        end = index+step
        dividedAr = raw_arr[index:end]
        index = end
        p = mp.Process(target=simplify_rdp, args=(i, return_pts, tolerance, dividedAr,))
        p.start()
        procList.append(p)

    for p in procList:
        p.join()

    final = return_pts[0]

    for i in range(1, q):
        final = np.concatenate((final, return_pts[i]))

    return final

def intersect_dictarray(d1, d2, simplify):
    out_dict = [{'meta':d2[0]['meta'], 'data':[]}]

    if(simplify): # Retira valores de alarmes. Tamanho de arquivo e tempo de execução 10% menores
        for o in d1:
            temp = {'secs':o[0], 'val':o[1], 'nanos': o[0]-math.floor(o[0])}
            out_dict[0]['data'].append(temp)
    else:
        cache = set(d1[:,0])

        print("Original point count: {}".format(len(d2[0]["data"])))
        print("Resulting point count: {}".format(len(d1)))

        # Passa dicts correspondentes aos valores mantidos pelo algoritmo
        for i in range(0,len(d2[0]["data"])):
            if d2[0]["data"][i]["secs"] in cache:
                out_dict[0]["data"].append(d2[0]["data"][i])

    return(out_dict)

@app.route('/retrieval/data/getData.json', methods=['GET', 'POST'])
def run_optimization():
    if request.method == 'GET':
        q_string = request.query_string.decode('utf-8')
        io_start = time.time()

        r = requests.get('http://ip/retrieval/data/getData.json?' + q_string)
        raw_array = []
        dict = r.json()

        io_end = time.time()
        print("Input Time: {} seconds".format(str(io_end - io_start)))
        calc_start = time.time()

        l_num = 0
        s_num = 9223372036854775807

        for i in dict[0]["data"]:
            raw_array.append([i["secs"], i["val"]])
            l_num = i["val"] if i["val"] > l_num else l_num
            s_num = i["val"] if i["val"] < s_num else s_num


        if(len(raw_array) > 4000):
            dif = l_num-s_num
            precision = int(dict[0]["meta"]["PREC"])

            if(dif*100/l_num > 70):
                tolerance = dif/50
            else:
                tolerance = dif/10000 if dif/10000 < 10**(precision*-1) else 10**(precision*-1) 

            # Metodologia: Busca obter uma tolerância aceitável baseada no intervalo entre o menor e maior números registrados,
            # com a tolerância sendo diretamente proporcional ao intervalo entre o maior e menor (menos pontos)

            print(tolerance)
            
            optAr = opt_rdp(1, raw_array, tolerance)
            resp = make_response(json.dumps(intersect_dictarray(optAr, dict, False)))
        else:
            resp = make_response(r.text)
        calc_end = time.time()
        print("Calculation Time: {} seconds".format(str(calc_end - calc_start)))

        resp.headers['Access-Control-Allow-Origin'] = '*'
        return resp
