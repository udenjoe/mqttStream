# mqttStream.  
Windows:   
Install Mosquitto broker https://mosquitto.org/download/  

Mac:  
brew install mosquitto.  
start and on login: brew services start mosquitto.  
one time: mosquitto -c /usr/local/etc/mosquitto/mosquitto.conf.   

Install paho-mqtt: python -m pip install paho-mqtt  

Start mosquitto  
Android Emulator localhost 10.0.2.2. 

Run Script with file path & name of file you wish to send  
Streams each line of a text file to MQTT   
