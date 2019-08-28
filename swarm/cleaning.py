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
    