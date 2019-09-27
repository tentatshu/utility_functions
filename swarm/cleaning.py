__author__ = “Connor Landy”

""" Helper functions leveraged in the process of cleaning, manipulating, 
    and generating data for the following project: 
    https://www.connorlandy.com/projects/my-amazing-race
"""

from pyspark.sql import functions as F

def parse_and_clean_swarm_checkins(swarms_output):
    """ Parse and clean raw nested Swarm dataframe

    Parameters
    ----------
    swarms_output: raw Swarm checkin data as dataframe where all checkin history
        is located in 1 row with 2 columnms: "count" and "items"

    Returns
    -------
    result : dataframe with the following 3 columns for each checkin: 
        "createdAt", 
        "id",
        "name"
    """
    result = swarms_output.select(F.explode(swarms_output.items).alias('items')).select('items.*') \
                .withColumnRenamed('id', 'checkin_id').select('*', 'venue.*') \
                .select('createdAt', 'id', 'name').withColumn('createdAt', F.col('createdAt').cast('timestamp'))
    return result


def parse_and_clean_swarm_venue_responses(check_in_response):
    """ Parse and clean raw nested JSON responses to the Foursquare GET VENUE_ID API

    Parameters
    ----------
    check_in_response: raw Swarm venue data as dataframe where every row is a separate 
        response to the GET venue_id API

    Returns
    -------
    result : dataframe with one row for every response that has the following columns:
        "id",
        "name",
        "address",
        "cc",
        "city",
        "country",
        "crossStreet",
        "isFuzzed",
        "lat",
        "lng",
        "lat_truncated",
        "lng_truncated"
        "neighborhood",
        "postalCode",
        "state" 
    
    Notes
    -----
    Foursquare API Call Documentation: https://developer.foursquare.com/docs/api/venues/details

    """
    # convert nested column to json struct
    json_schema = spark.read.json(check_in_response.rdd.map(lambda row: row.response)).schema
    result = check_in_response.withColumn('json', F.from_json(F.col('response'), json_schema))

    # parse response
    result = result.select('json.*').select('response.*').select('venue.*') \
        .select('id', 'name', 'location.*').drop('formattedAddress', 'labeledLatLngs') \
        .withColumn('lat_truncated', F.format_number(F.col('lat'), 2)) \
        .withColumn('lng_truncated', F.format_number(F.col('lng'), 2))
    return result


from math import sin, cos, sqrt, atan2, radians
from pyspark.sql.window import Window
import pyspark.sql.types as types

def calculate_distance(enhanced_checkin_data):
    """ Create distance (km) column in PySpark dataframe 

    Parameters
    ----------
    enhanced_checkin_data: Dataframe that has gone through parse_and_clean_swarm_venue_responses function or
        has following equivalent columns: created_at, lat, lng
   

    Returns
    -------
    Same dataframe that has an additional column called distance_in_km
    
    Notes
    -----
    

    """
    # remove records that do not have geocoordinates 
    result = enhanced_checkin_data.filter(F.col('lat').isNotNull())
    
    # add prior latitude and longitude as new columns for every record
    w1 = Window.orderBy(result.createdAt.asc())
    result = result.withColumn('prior_latitude', F.lag(F.col('lat'), 1).over(w1)) \
        .withColumn('prior_longitude', F.lag(F.col('lng'), 1).over(w1)) \
        .withColumn('prior_name', F.lag(F.col('name'), 1).over(w1)) \
        .withColumn('prior_country', F.lag(F.col('country'), 1).over(w1))
    
    # remove the first data point for the calculation given there is no distance to be performed
    result = result.filter(F.col('prior_latitude').isNotNull())

    # calculate distance in km leveraging udf define below
    result = result.withColumn('distance_in_km', calculate_distance_udf('lat', 'lng', 'prior_latitude', 'prior_longitude'))

    return result 


def calculate_distance_function(lat_1, long_1, lat_2, long_2):
    """ PySpark UDF to define distance between 2 points based on latitude and longitude

    Parameters
    ----------
    lat_1: latitude of first datapoint
    long_1: longitude of first datapoint
    lat_2: latitude of second datapoint
    long_2: longitude of second datapoint
    

    Returns
    -------
    Distance in KM
    
    
    Notes
    -----
    Source: https://www.movable-type.co.uk/scripts/latlong.html
    Must be registered with udf function to utilize in PySpark.

    """
    R = 6373.0
    
    lat1 = radians(lat_1)
    lon1 = radians(long_1)
    lat2 = radians(lat_2)
    lon2 = radians(long_2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance

# register above function as PySpark UDF
calculate_distance_udf = F.udf(calculate_distance_function, types.FloatType())
    
