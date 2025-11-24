from confluent_kafka import Producer
import time , requests, json

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config


def produce(topic, config):
    # create a new producer instance
    producer = Producer(config)

    URL = "https://opensky-network.org/api/states/all"

    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Produced to {msg.topic()} partition {msg.partition()} offset {msg.offset()}")

    while True:
        try:
            response = requests.get(URL)
            response.raise_for_status()
            data = response.json()
            states = data.get("states", [])
            
            for s in states:
                doc = {
                    "icao24": s[0],
                    "callsign": s[1].strip() if s[1] else None,
                    "origin_country": s[2],
                    "time_position": s[3],
                    "last_contact": s[4],
                    "longitude": s[5],
                    "latitude": s[6],
                    "geo_altitude": s[7],
                    "on_ground": s[8],
                    "velocity": s[9],
                    "heading": s[10],
                    "vertical_rate": s[11],
                    "baro_altitude": s[13],
                    "squawk": s[14],
                    "spi": s[15],
                    "position_source": s[16],
                    "ingest_time": int(time.time())
                }
                producer.produce(topic, key=doc["icao24"], value=json.dumps(doc), callback=delivery_report)
            
            producer.flush()
            print(f"Produced {len(states)} messages to topic {topic}")
            time.sleep(5)

        except Exception as e:
            print(f"Error fetching or producing data: {e}")
            time.sleep(5)

def main():
  config = read_config()
  topic = "opensky-flights"

  produce(topic, config)


main()