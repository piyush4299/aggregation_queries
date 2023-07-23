import asyncio
import json
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import FastAPI
from bson import ObjectId


async def ping_server(client):
    # Send a ping to confirm a successful connection
    try:
        await client.admin.command('ping')
        print("Pinged your deployment. Connection with Mongodb seems to be good!!")
    except Exception as e:
        print(e)


async def get_client():
    uri = "mongodb+srv://<username>:<password>@cluster001.jb6pvph.mongodb.net/"
    # Create a new client and connect to the server
    client = AsyncIOMotorClient(uri)
    await ping_server(client)
    return client


async def get_collection(dbName, collectionName):
    client = await get_client()
    db = client[dbName]
    return db[collectionName]

app = FastAPI()


@app.get("/")
async def get_overview():
    return "Sample App for practicing aggregation queries"

@app.get("/firstQuery")
async def first_query():
    print('In db sample_mflix, collection movies, find the unique countries.')
    collection = await get_collection('sample_mflix', 'movies')
    try:
        cursor = collection.aggregate([
            {"$unwind": "$countries"},
            {"$group": {"_id": None, "uniqueCountries": {"$addToSet": "$countries"}}},
            {"$project": {
                "_id": 0,
                "uniqueCountries": 1,
                "uniqueCountriesCount": {"$size": "$uniqueCountries"}
            }
            }
        ])

        first_query_response_list = await cursor.to_list(None)
        print('firstQuery response: ', first_query_response_list)
        return {'status': 200, 'message': 'success', 'firstQueryResponse': str(first_query_response_list)}
    except Exception as e:
        print("Something went wrong with fetching firstQuery: ", e)
        return {'status': 500, 'message': 'Something went wrong'}


@app.get("/secondQuery")
async def second_query():
    print('In db sample_mflix, collection movies, find the unique genres for country France.')
    collection = await get_collection('sample_mflix', 'movies')
    try:
        cursor = collection.aggregate([
            {"$match": {"countries": "France"}},
            {"$unwind": "$genres"},
            {"$group": {"_id": None, "uniqueGenres": {"$addToSet": "$genres"}}},
            {"$project": {
                "_id": 0,
                "uniqueGenres": 1,
                "uniqueGenresCnt": {"$size": "$uniqueGenres"}
            }
            }
        ])

        second_query_response_list = await cursor.to_list(None)
        print('secondQuery response: ', second_query_response_list)
        return {'status': 200, 'message': 'success', 'secondQueryResponse': str(second_query_response_list)}
    except Exception as e:
        print("Something went wrong with fetching secondQuery: ", e)
        return {'status': 500, 'message': 'Something went wrong'}


@app.get("/thirdQuery")
async def third_query():
    print('In db sample_mflix, collection movies, find all movies having imdb rating more than 7 and at least 10 reviews and tomatoes rating more than 8 with at least 50 reviews.')
    collection = await get_collection('sample_mflix', 'movies')
    try:
        cursor = collection.aggregate([
            {"$match": {"$and": [{"imdb.rating": {"$gt": 7}}, {"imdb.votes": {"$gt": 10}}, {
                "tomatoes.rotten": {"$gt": 8}}, {"tomatoes.viewer.numReviews": {"$gt": 50}}]}},
            {
                "$project": {
                    "_id": 0,
                    "title": "$title",
                    "imdb": "$imdb",
                    "tomatoes-viewer-obj": "$tomatoes.viewer",
                    "rottenTomatoes": "$tomatoes.rotten",
                }
            }
        ])

        third_query_response_list = await cursor.to_list(None)
        print('thirdQuery response: ', third_query_response_list)
        return {'status': 200, 'message': 'success', 'thirdQueryResponse': str(third_query_response_list)}
    except Exception as e:
        print("Something went wrong with fetching thirdQuery: ", e)
        return {'status': 500, 'message': 'Something went wrong'}


@app.get("/fourthQuery")
async def fourth_query():
    print('In db sample_mflix, collection movies, find the decade (1960, 1970, 1980, etc...) in which the most movies were released.')
    collection = await get_collection('sample_mflix', 'movies')
    try:
        cursor = collection.aggregate([
            {"$match": {"year": {"$type": "int"}}},
            {"$group": {"_id": {"$floor": {"$divide": [
                {"$subtract": [{"$toInt": "$year"}, 1900]}, 10]}}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1},
            {
                "$project": {
                    "_id": 0,
                    "decade": {"$sum": [{"$multiply": ["$_id", 10]}, 1900]},
                    "maxMovieCount": "$count"
                }
            }
        ])

        fourth_query_response_list = await cursor.to_list(None)
        print('fourthQuery response: ', fourth_query_response_list)
        return {'status': 200, 'message': 'success', 'fourthQueryResponse': str(fourth_query_response_list)}
    except Exception as e:
        print("Something went wrong with fetching fourthQuery: ", e)
        return {'status': 500, 'message': 'Something went wrong'}


@app.get("/fifthQuery")
async def fifth_query():
    print('In db sample_mflix, collections movies and comments, find the name/title of the movie having the most comments.')
    movies_collection = await get_collection('sample_mflix', 'movies')
    comments_collection = await get_collection('sample_mflix', 'comments')
    try:
        cursor = comments_collection.aggregate([
            {"$group": {"_id": "$movie_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$lookup": {
                "from": "movies",
                "localField": "_id",
                "foreignField": "_id",
                "as": "movie_details",
            }},
            {"$limit": 1},
            {"$unwind": "$movie_details"},
            {"$project": {
                "_id": "$movie_details._id",
                "title": "$movie_details.title",
                "count": 1
            }}
        ])

        fifth_query_response_list = await cursor.to_list(None)
        print('fifthQuery response: ', fifth_query_response_list)
        return {'status': 200, 'message': 'success', 'fifthQueryResponse': str(fifth_query_response_list)}
    except Exception as e:
        print("Something went wrong with fetching fifthQuery: ", e)
        return {'status': 500, 'message': 'Something went wrong'}


@app.get("/sixthQuery")
async def sixth_query():
    print('In db sample_airbnb, collection listingsAndReviews, find the average price of all properties in Porto market (address.market).')
    collection = await get_collection('sample_airbnb', 'listingsAndReviews')

    try:
        cursor = collection.aggregate([
            {"$match": {"address.market": "Porto"}},
            {"$group": {"_id": "$address.market", "averageAmt": {
                "$avg": "$price"}, "count": {"$sum": 1}}},
            {"$project": {
                "_id": "_id",
                "totalPlaceConsidered": "$count",
                "averageAmount": "$averageAmt"
            }}
        ])

        sixth_query_response_list = await cursor.to_list(None)
        print('sixthQuery response: ', sixth_query_response_list)
        return {'status': 200, 'message': 'success', 'sixthQueryResponse': str(sixth_query_response_list)}
    except Exception as e:
        print("Something went wrong with fetching fifthQuery: ", e)
        return {'status': 500, 'message': 'Something went wrong'}


@app.get("/seventhQuery")
async def seventh_query():
    print(
        'In db sample_airbnb, collection listingsAndReviews, find the number of properties in 10km range of the coordinates - [-8.61308, 41.1413] (This is long first, then lat) (Hint: check geo stages and operators in MongoDB).')
    collection = await get_collection('sample_airbnb', 'listingsAndReviews')

    try:
        cursor = collection.aggregate([
            {
                "$geoNear": {
                    "near": {
                            "type": "Point",
                            "coordinates": [-8.61308, 41.1413]
                        },
                        "maxDistance": 10000,
                        "distanceField": "dist.calculated",
                        "includeLocs": "address.location.coordinates",
                        "spherical": True,
                    }
            },
            {
                "$group": {
                    "_id": None,
                    "count": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "numberOfPropertiesInRange": "$count"
                }
            }

        ])

        seventh_query_response_list = await cursor.to_list(None)
        print('seventhQuery response: ', seventh_query_response_list)
        return {'status': 200, 'message': 'success', 'seventhQueryResponse': str(seventh_query_response_list)}
    except Exception as e:
        print("Something went wrong with fetching fifthQuery: ", e)
        return {'status': 500, 'message': 'Something went wrong'}


@app.get("/eighthQuery")
async def eighth_query():
    print('In db sample_restaurants, collection restaurants, find the top 5 restaurants with highest average grade score -- (grades.score)')
    collection = await get_collection('sample_restaurants', 'restaurants')

    try:
        cursor = collection.aggregate([
            {"$unwind": "$grades"},
            {"$group": {
                "_id": "$name",
                "averageScore": {"$avg": "$grades.score"},
            }},
            {"$sort": {"averageScore": -1}},
            {"$limit": 5},
            {
                "$project": {
                    "_id": 0,
                    "name": "$_id",
                    "averageScore": "$averageScore"
                }
            }
        ])

        eighth_query_response_list = await cursor.to_list(None)
        print('eighthQuery response: ', eighth_query_response_list)
        return {'status': 200, 'message': 'success', 'eighthQueryResponse': str(eighth_query_response_list)}
    except Exception as e:
        print("Something went wrong with fetching fifthQuery: ", e)
        return {'status': 500, 'message': 'Something went wrong'}