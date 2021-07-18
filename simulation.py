"""
Ce dummy producer va récupérer des trajets en 2019-03 à la même heure, minute et seconde afin
de simuler des trajets effectués en réel.
"""
import os
import sys
import json
import time
import warnings
import pandas as pd
import logging
from datetime import datetime, timedelta
# sudo pip3 install pandas confluent-kafka
# sudo pip3 install "confluent-kafka[azvro]"
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False
fh = logging.FileHandler("/tmp/log_simulation.log", "a+")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
start_date = datetime.now()
warnings.filterwarnings('ignore')
# Remplacer par le serveur Confluent
CONFLUENT_URL = "34.140.187.223:9092"
SCHEMA_REGISTRY_URL = "http://34.140.187.223:8081"

# on ne peut pas sérialiser par défaut un DateTime Python
def json_converter(o):
        if isinstance(0, datetime):
                return o.__str__()
# Les colonnes sources du fichier
source_columns = "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime," \
        + "passenger_count,trip_distance,RatecodeID," \
        + "payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge," \
        + "total_amount,PULocationID,DOLocationID"
source_columns = source_columns.split(",")
# Les colonnes cibles dans BigQuery
target_columns = "vendor_id,pickup_datetime,dropoff_datetime,passenger_count," \
        + "trip_distance, rate_code," \
        + "payment_type,fare_amount,extra,mta_tax,tip_amount," \
        + "tolls_amount,improvement_surcharge,total_amount,PULocationID,DOLocationID"
target_columns = target_columns.split(",")
data = None
print("Chargement des données ...")

for chunk in pd.read_csv(
	"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-03.csv",
	chunksize=1000000):
	data = chunk
	break

# On récupère les informations temporelles pour simuler des données temps réel
data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
data['tpep_dropoff_datetime'] = pd.to_datetime(data['tpep_dropoff_datetime'])
data['duration'] = pd.to_timedelta(data['tpep_dropoff_datetime'] - data['tpep_pickup_datetime']) 
data['pickup_hour'] = data['tpep_pickup_datetime'].dt.hour
data['pickup_minute'] = data['tpep_pickup_datetime'].dt.minute
data['pickup_second'] = data['tpep_pickup_datetime'].dt.second
data['dropoff_hour'] = data['tpep_dropoff_datetime'].dt.hour
data['dropoff_minute'] = data['tpep_dropoff_datetime'].dt.minute
data['dropoff_second'] = data['tpep_dropoff_datetime'].dt.second

def get_current_trips():
        now = datetime.now()
	# trips['tpep_dropoff_datetime'] == datetime.now()
        trips = data.loc[
                (data['dropoff_hour'] == now.hour) &
                (data['dropoff_minute'] == now.minute) &
                (data['dropoff_second'] == now.second)
	, :]
        trips['tpep_dropoff_datetime'] == datetime.now()
       	trips['VendorID'] = trips['VendorID'].astype(str)
        # Conversion en timestamp UNIX
        trips['tpep_pickup_datetime'] = (trips['tpep_dropoff_datetime'] - trips['duration']).values.astype(int) // 10 ** 9
        trips['tpep_dropoff_datetime'] = trips['tpep_dropoff_datetime'].values.astype(int) // 10 ** 9
        trips = trips[source_columns]
        trips.columns = target_columns
        return trips

schema = """{
    "type": "record",
    "name": "ongoing_trips_schema",
    "namespace": "com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever.SchemaRegistrySchemaRetriever",
    "fields": [
        {"name": "vendor_id", "type": ["string", "null"]},
        {"name": "pickup_datetime", "type": ["int", "null"]},
        {"name": "dropoff_datetime", "type": ["int", "null"]},
        {"name": "passenger_count", "type": ["int", "null"]},
        {"name": "trip_distance", "type": ["float", "null"]},
        {"name": "rate_code", "type": ["int", "null"]},
        {"name": "payment_type", "type": ["int", "null"]},
        {"name": "fare_amount", "type": ["float", "null"]},
        {"name": "extra", "type": ["float", "null"]},
        {"name": "mta_tax", "type": ["float", "null"]},
        {"name": "tip_amount", "type": ["float", "null"]},
        {"name": "tolls_amount", "type": ["float", "null"]},
        {"name": "improvement_surcharge", "type": ["float", "null"]},
        {"name": "total_amount", "type": ["float", "null"]},
        {"name": "PULocationID", "type": ["int", "null"]},
        {"name": "DOLocationID", "type": ["int", "null"]}
   ]
}"""

avro_schema = avro.loads(schema)
producer = AvroProducer({
        'bootstrap.servers': CONFLUENT_URL,
        'schema.registry.url': SCHEMA_REGISTRY_URL
}, default_value_schema=avro_schema)
start_time = time.time()

logger.info("Start producing ...")
print("démarrage du PRODUCER.......")

keep_producing = True
i = 0
while keep_producing:
        try:
            trips = get_current_trips()
            # iterrows return une tuple (index, Series)
            for _, trip in trips.iterrows():
                    producer.produce(
                            topic='ongoing-trips',
                            value=trip.to_dict())
            # On augmente le délai pour ne pas saturer la bande passante (économie de crédits)
            time.sleep(5.0 - ((time.time() - start_time) % 5.0))
            keep_producing = datetime.now() <= start_date + timedelta(hours=1)
        except Exception as e:
            logger.error("Error :", e)
logger.info("Fin de la simulation.")
# A la fin de la simulation, une tâche CRON va relancer ce programme
# au bout d'une heure le programme s'arrête
# il sera relancé automatiquement par une tâche crône
