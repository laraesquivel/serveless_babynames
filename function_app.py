#Azure imports
import azure.functions as func
import logging

#Mongo imports
from pymongo.mongo_client import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo import GEOSPHERE, GEO2D
from bson import json_util, Timestamp

#Utils Base imports
from dotenv import load_dotenv
import os
from datetime import datetime
import json
from random import randint

load_dotenv()
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
mongo_client = MongoClient(os.getenv('URI_MONGO'))

@app.route(route="getNames")
def get_names(req: func.HttpRequest) -> func.HttpResponse:
        
    database = None

    logging.info('Python HTTP trigger function processed a request.')
    logging.info(req.method)
    try:
        database = mongo_client['babynames']
    except ConnectionError as e:
        # Se ocorrer um erro de conexão, retorne um erro interno do servidor (HTTP 500)
        return func.HttpResponse(f"Erro de conexão com o banco de dados: {str(e)}", status_code=500)
    except Exception as e:
        # Outros erros podem ser tratados aqui
        return func.HttpResponse(f"Erro interno do servidor: {str(e)}", status_code=500)
    
    name = req.params.get('name')

    if req.method != 'GET':
        return func.HttpResponse(f"Method not allow, rote just suported GET method",status_code=405)

    if name:
        resultados = database['names'].find_one({'name':name})
        resultados = json_util.dumps(resultados, default=str)
        return func.HttpResponse(resultados,status_code=200)
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=400
        )
    
@app.route("postAction")
def post_action(req: func.HttpRequest) -> func.HttpResponse:
    req_body = None
    date_hour = datetime.utcnow()
    timestamp = Timestamp(int(date_hour.timestamp()), 0)

    try:
        req_body = req.get_json()
        req_body['timestamp'] = timestamp

    except ValueError:
        return func.HttpResponse("Invalid Req_Body", status_code=400)

    try:
        database = mongo_client["babynames"]
        collection_actions = database['actions']


        result = collection_actions.insert_one(req_body)
        return func.HttpResponse(json.dumps({'message': 'Sucess in insert a new document','id' : str(result.inserted_id)}), status_code=201)

    except ConnectionFailure as e:
        return func.HttpResponse(f"Error with database connection: {str(e)}", status_code=503)
    except Exception as e:
        return func.HttpResponse(f"Error in insert a new document in collection: {str(e)}", status_code=500)

    

@app.route("getrecPhrase")
def rec_phrase(req: func.HttpRequest) -> func.HttpResponse:
    user_id = req.params.get('userId')
    if user_id:
        try:
            database = mongo_client['babynames']
            users_collection = database['users']
            phrases_collection = database['phrases']
            parcial_response = users_collection.find_one({'tokenId':user_id})
            if user_id and 'nextPhrases' in parcial_response:
                arr_phrases = parcial_response['nextPhrases']
                if arr_phrases:
                    random_index = randint(0,len(arr_phrases) - 1)
                    response = json.dumps({'phrase' : arr_phrases[random_index], 'message': 'Its all okay'})
                    return func.HttpResponse(response, status_code=200)
            randon_phrase = json_util.dumps(phrases_collection.aggregate([{"$sample":{"size":1}}]).next())
            return func.HttpResponse(randon_phrase,status_code=200,mimetype="application/json")


        except ConnectionFailure as e:
            return func.HttpResponse(json.dumps({"message":e}), status_code=503)

        except Exception as e:
            return func.HttpResponse(json.dumps({'message':e}),status_code=500)
    return func.HttpResponse(json({'menssage':  "This HTTP triggered function executed successfully. Pass a userId in the query string or in the request body for a personalized response."}),
             status_code=400)

@app.route("postNewUser")
def post_new_user(req: func.HttpRequest) -> func.HttpResponse:
    user_id = None
    try:
        req_body = req.get_json()
        user_id = req_body['userId']
    except (ValueError, KeyError):
        return func.HttpResponse("Invalid Req_Body or missing 'userId'", status_code=400)

    if user_id:
        try:
            database = mongo_client['babynames']
            users_collection = database['users']
            result = users_collection.insert_one({'tokenId': user_id})
            return func.HttpResponse(json.dumps({'message': 'Add new user', 'userId': str(result.inserted_id)}),
                                     status_code=201, mimetype="application/json")
        except Exception as e:
            return func.HttpResponse(json.dumps({'error': str(e)}), status_code=500, mimetype="application/json")
    
    return func.HttpResponse(json.dumps({'message': 'Bad Request, userId is missing!'}), status_code=400, mimetype="application/json")

@app.route("getNamesToPhrase")
def get_names_to_phrase(req : func.HttpRequest)-> func.HttpResponse:
    phrase = req.params.get('phrase')
    if phrase:
        try:
            database = mongo_client['babynames']
            phrases_collection = database['phrases']
            response = phrases_collection.find_one({'phrase':phrase})
        except ConnectionFailure as e:
            return func.HttpResponse(json({'message':'We cant connect to database'}),status_code=503)
        except Exception as e:
            return func.HttpResponse(json({'message':'Some error ocurred!'}),status_code=500)
    return func.HttpResponse(json({'message':'Bad Request, phrase is missing!'}),status_code=400)


@app.timer_trigger(schedule="mongo_pipeline_timer_trigger", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def mongo_pipeline_timer_trigger(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')