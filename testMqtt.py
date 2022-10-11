import paho.mqtt.client as mqtt
import os
import time
import logging
from datetime import datetime
import sys
from decimal import Decimal
from math import radians, cos, sin, asin, sqrt, atan2
from sys import platform

#for windows
#install mosquitto broker https://mosquitto.org/download/
#Then run mosquitto from directory
#Todo: start mosquitto from here

#TODO
fileSeperator = '\\'
if platform == "darwin":
    fileSeperator = '/'

messages_list = ["STREAM_DATA", "CLEAR_TRACE", "SIGNALS_DATA", "WRITE_FILE_NAME", "SETUP_AVERAGES"] #mqtt topics

# Logging configuration
logfilename = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(level=logging.DEBUG, filename=logfilename + ".log", filemode='w', format='%(asctime)s - %(filename)s:%(lineno)d %(name)s - %(levelname)s - %(message)s')
stderrLogger=logging.StreamHandler()
stderrLogger.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
logging.getLogger().addHandler(stderrLogger)

# Log levels for Paho MQTT client.py107
MQTT_LOG_INFO = 0x01
MQTT_LOG_NOTICE = 0x02
MQTT_LOG_WARNING = 0x04
MQTT_LOG_ERR = 0x08
MQTT_LOG_DEBUG = 0x10
LOGGING_LEVEL = {
    MQTT_LOG_DEBUG: logging.DEBUG,
    MQTT_LOG_INFO: logging.INFO,
    MQTT_LOG_NOTICE: logging.INFO,  # This has no direct equivalent level
    MQTT_LOG_WARNING: logging.WARNING,
    MQTT_LOG_ERR: logging.ERROR,
}

def haversine(lon1, lat1, lon2, lat2):
    r = 6371
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)
    a = sin(delta_phi / 2)**2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2)**2
    res = r * (2 * atan2(sqrt(a), sqrt(1 - a)))
    return round(res, 2)

def subscribe_mqtt_signals():
    for message in messages_list:
        client.subscribe(message)

def on_connect(client, userdata, flags, rc):
    logging.info("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    logging.info("On Connect")
    subscribe_mqtt_signals()

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    logging.info(msg.topic+" "+str(msg.payload))
    #if msg.topic == "REQUEST_DATA": #TODO: start streaming when Android sends this command?
     #   client.publish("CONFIG_PUSH", config_contents)

def on_log(mqttc, obj, level, string):
    logging.info('PahoMQTT-%s: %s',LOGGING_LEVEL[level], string)

def read_trace(filePath, option):
    tracefile = open(filePath, "r")
    traceline = tracefile.readline()

    invalid_list = [':', '\\', '/', '?', '*', '[', ']', '"'] #invalid chars for excel output

    #Handle different file formats
    counter = 0
    isRoadLoads = "#HEADER" in traceline
    isMoTeC = "MoTeC" in traceline
    signalsHeader = 2
    dataStart = 8    
    latIndex = 4
    lonIndex = 5
    distIndex = 1
    if(isMoTeC):
        signalsHeader = 14
        dataStart = 17
        latIndex = 47
        lonIndex = 48

    logging.debug("signalsHeader:" + str(signalsHeader) + " data start:" + str(dataStart))

    lastLatitude = "0.0"
    lastLongitude = "0.0"
    skippedLines = 0
    distValue = "-1"

    #handle output file size
    validLines = 0
    outputCount = 0
    passFlag = 1
    #'\\'
    logging.debug(fileName)
    outputFile = fileName[fileName.rindex(fileSeperator) + 1:len(fileName) - 4] + "_" + logfilename #need first index + 1, -4 for extension
    if(option is not None):
        outputFile = outputFile + "_F"

    if(distIndex == 1):
        outputFile = outputFile + "_Dist"

    if(distIndex == 2):
        outputFile = outputFile + "_ODO"


    while traceline:
        #logging.debug(str(counter) + ":" + traceline[0:70])
        while(counter < dataStart):
            logging.debug("Count to header: " + str(counter))
            traceline = tracefile.readline()
            counter += 1

            if(counter == signalsHeader):
                logging.debug("Send Signals")
                for invalid in invalid_list:
                    traceline = traceline.replace(invalid, '')

                traceline = traceline.strip('\n')
                client.publish("SIGNALS_DATA", traceline)#Send the signals data line from csv files
                traceline = tracefile.readline()
                counter += 1
                continue

        if(isMoTeC):
            traceline = traceline.replace('"', '')
            traceline = traceline.strip('\n')

        separated = traceline.split(",")
        if(len(separated) <= 1):
            logging.debug("Non empty line: " + str(len(separated)))
            traceline = tracefile.readline()
            counter += 1
            continue

        if(isMoTeC): #use odo to filter
            thisiDistValue = separated[distIndex]
            # logging.debug("Dist last: " + distValue + "->this: " + thisiDistValue)
            if(distValue == thisiDistValue):
                traceline = tracefile.readline()
                counter += 1
                continue
            distValue = thisiDistValue

        thisLat = 0.0
        thisLon = 0.0
        change = 0

        if(isRoadLoads):
            thisLat = separated[latIndex]
            thisLon = separated[lonIndex]
        
        if(isMoTeC):
            thisLat = Decimal(separated[latIndex])
            thisLon = Decimal(separated[lonIndex])
        
        #logging.debug("lat:" + str(thisLat) + " lon:" + str(thisLon))
        if((lastLatitude == thisLat) and (lastLongitude == thisLon)):
            traceline = tracefile.readline()
            lastLatitude = thisLat
            lastLongitude = thisLon
            continue

        distance = haversine(Decimal(lastLongitude), Decimal(lastLatitude), Decimal(thisLon), Decimal(thisLat))
        #logging.debug("distance:" + str(distance))
        if(distance <= 0.001):
            traceline = tracefile.readline()
            counter += 1
            # lastLatitude = thisLat
            # lastLongitude = thisLon
            continue

        lastLatitude = thisLat
        lastLongitude = thisLon
        if(passFlag > 0):
            client.publish("STREAM_DATA", traceline)
            logging.debug("sending: " + traceline)
            validLines += 1
        traceline = tracefile.readline()
        
        counter+=1
        if(validLines == 1000): #1000
            client.publish("WRITE_FILE_NAME", outputFile + "_" + str(outputCount).zfill(2))
            outputCount += 1
            validLines = 0

        if(isMoTeC):
            passFlag *= -1 
        time.sleep(0.20)

    tracefile.close()
    client.publish("WRITE_FILE_NAME", outputFile + "_" + str(outputCount).zfill(2))

#connect MQTT Client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log
client.connect("127.0.0.1", 1883, 60)
client.loop_start()

#Start sending file
client.publish("CLEAR_TRACE")#clean up map
client.publish("SETUP_AVERAGES", "1,0,0")#send the average stuff
fileName = str(sys.argv[1])
option = None
if(len(sys.argv) > 2):
    option = sys.argv[2]
read_trace(fileName, option)
