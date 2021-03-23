import http.client
import json
import datetime
import csv
import logging
import os
import cloudstorage as gcs
from google.appengine.api import app_identity

ACCESS_TOKEN = 'access-token'
HOST_URL = 'api.football-data.org'
END_POINT = '/v2/matches?'
REQUEST_TYPE = 'GET'
INTERVAL = 7
DATE_FORMAT = "%Y-%m-%d"


def get_7th_date_from_current_date():
    """
    Get the 7th date from the current date.
    Note: For the demo purpose the date is hardcoded.
    """
    currentDate = datetime.datetime.strptime("2020-09-09", "%Y-%m-%d")
    seventhDay = datetime.timedelta(days=INTERVAL)
    return currentDate + seventhDay


def create_network_connection(dateFrom, dateTo):
    """
    Get the result of the matches which was performed for the week.
    The paramters which should be passed are dateFrom and dateTo
    Note: In this case the parameters are hardcoded as I was not able to retrieve with the latest date,
    :return: connection object
    """
    connection = http.client.HTTPConnection(HOST_URL)
    headers = {'X-Auth-Token': ACCESS_TOKEN}
    connection.request('GET', END_POINT + 'dateFrom=2020-09-09&dateTo=2020-09-16', None, headers)
    return connection


def write_csv_to_gcs(response):
    """
    Once we have the response we need to create a csv file and store it in the GCS
    :param response: the response from the connection
    """
    try:
        bucket_name = os.environ.get('BUCKET_NAME', app_identity.get_default_gcs_bucket_name())
        filename = os.path.join(bucket_name, 'myfile.csv')
        write_retry_params = gcs.RetryParams(backoff_factor=1.1)
        gcs_file = gcs.open(filename, 'w', content_type='text/csv', retry_params=write_retry_params)
        writer = csv.writer(gcs_file, delimiter='\t')
        writer.writerow(
            ['Date', 'league', 'season', 'home_team_name', 'home_team_goals', 'away_team_goals', 'away_team_goals'])
        for match in response['matches']:
            writer.writerow([match['utcDate'],
                             match['competition']['name'],
                             match['season']['id'],
                             match['homeTeam']['name'],
                             match['score']['fullTime']['homeTeam'],
                             match['awayTeam']['name'],
                             match['score']['fullTime']['awayTeam']])
        gcs_file.close()
    except Exception as e:
        logging.error("Error when writing csv file to google cloud storage")


if __name__ == '__main__':
    #  Calculate the 7th date from the current date
    date_interval = get_7th_date_from_current_date()
    #  Create a network connection object
    connection = create_network_connection(datetime.date.today(), date_interval)
    #  Return network response
    response = json.loads(connection.getresponse().read().decode())
    #  Write the response to CSV file in google cloud storage
    write_csv_to_gcs(response)

