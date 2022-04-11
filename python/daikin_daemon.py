from decimal import DivisionByZero
import time
import json
import paho.mqtt.client as mqtt
import pytz
import logging
from logging.handlers import RotatingFileHandler
from datetime import date, datetime
from influxdb import InfluxDBClient

##GLOBAL VARIABLES####

daikin_db_name = "daikin"
power_db_name = "power"

log_level = logging.DEBUG
interval = 10 #Seconds
daikin_topic = "espaltherma/ATTR"
mqtt_server = "192.168.2.2"


def create_logger(log_file_name, log_level):
    """
        Create the logger for the script.

       :returns: logger, log_handler Objects properly configured.
       :rtype: tuple
    """
    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    log_handler = RotatingFileHandler(log_file_name, maxBytes=20000000,
                                      backupCount=5)
    log_handler.setFormatter(formatter)
    logger.setLevel(log_level)
    # Enable the screen logging.
    logger.addHandler(log_handler)
    console = logging.StreamHandler()
    console.setLevel(log_level)
    logger.addHandler(console)
    return logger, log_handler

def on_connect(client, userdata, flags, rc):
    logger.info("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(daikin_topic)

def on_message(client, userdata, msg):
    logger.debug(msg.topic+" "+str(msg.payload))
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    m_in=json.loads(m_decode) #decode json data
    """m_in.pop("M5VIN")
    m_in.pop("M5AmpIn")
    m_in.pop("M5BatV")
    m_in.pop("M5BatCur")
    m_in.pop("M5BatPwr")"""
    m_in.pop("WifiRSSI")
    logger.debug(m_in)
    #Formatting ints to floats:
    m_in["Outdoor air temp.(R1T)"] = float(m_in["Outdoor air temp.(R1T)"])
    m_in["INV primary current (A)"] = float(m_in["INV primary current (A)"])
    #m_in["INV secondary current (A)"] = float(m_in["INV secondary current (A)"])
    m_in["Leaving water temp. before BUH (R1T)"] = float(m_in["Leaving water temp. before BUH (R1T)"])
    m_in["Leaving water temp. after BUH (R2T)"] = float(m_in["Leaving water temp. after BUH (R2T)"])
    m_in["Refrig. Temp. liquid side (R3T)"] = float(m_in["Refrig. Temp. liquid side (R3T)"])
    m_in["Inlet water temp.(R4T)"] = float(m_in["Inlet water temp.(R4T)"])
    m_in["DHW tank temp. (R5T)"] = float(m_in["DHW tank temp. (R5T)"])
    m_in["Flow sensor (l/min)"] = float(m_in["Flow sensor (l/min)"])
    m_in["Mixed water temp.(R7T)"] = float(m_in["Mixed water temp.(R7T)"])
    #Get the last reading on voltage for the power calculation
    query = 'SELECT last("voltage") FROM "ev_power"'
    results = power_db_client.query(query)
    voltage = 230.0
    for result in results:
        voltage = float(result[0]["last"])
    m_in["INV Power"] = m_in["INV primary current (A)"] * voltage
    m_in["Voltage"] = float(voltage)
    #Calculating COP:
    if m_in["Freeze Protection"] == "OFF" and m_in["Operation Mode"] == "Heating" and m_in["INV primary current (A)"] > 0.0:
        dividend = m_in["Flow sensor (l/min)"] * 0.06 * 1.16 * (m_in["Leaving water temp. before BUH (R1T)"] - m_in["Leaving water temp. after BUH (R2T)"])
        divisor = m_in["INV Power"] / 1000 #KWatts
        cop = abs(round(dividend / divisor ,2))
        m_in["COP"] = cop
    timestamp = datetime.utcnow().replace(tzinfo=pytz.utc)
    json_body = [
                                    {
                                        "measurement": "daikin",
                                        "tags": {
                                            "user": "Alex"
                                        },
                                        "time": timestamp.isoformat(),
                                        "fields": m_in
                                    }
                                ]
    daikin_db_client.write_points(json_body)

def create_db(db_name):
    client = InfluxDBClient(host='localhost', port=8086)
    db_list = client.get_list_database()
    for db in db_list:
        if db['name'] == db_name:
            logger.debug(f"Database {db_name} already exists, skipping creation")
            return
    client.create_database(db_name)

if __name__ == '__main__':
    logger, log_handler = create_logger("./logs/daiking.log",log_level)
    logger.info("Starting daiking measurements...")
    try:
        logger.info("Initializing Influx DB")
        create_db(daikin_db_name)
        daikin_db_client = InfluxDBClient(host='localhost', port=8086,database=daikin_db_name)
        # Needed for lack of voltage input :-(
        power_db_client = InfluxDBClient(host='localhost', port=8086,database=power_db_name)
        logger.info("Initializing Mosquitto client.")
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        logger.info("Starting Mosquitto client.")
        while True:
            try:
                client.connect(mqtt_server, 1883, 60)
                logger.info("Client connected, starting loop")
                client.loop_forever()
            except:
                logger.error("Generic exception, will wait 30 seconds to try again")
                time.sleep(10)
                pass
                
    except KeyboardInterrupt:
        logger.info("Daiking daemon finished via keyboard interrupt.")
 
    
    