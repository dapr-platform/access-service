package config

import "os"

var MQTT_GATEWAY_TOPIC = "/gateway/#"
var MQTT_DEVICE_TOPIC = "/device/#"
var MQTT_QOS = 0

var MQTT_BROKER = "emqx:1883"
var MQTT_USER = "admin"
var MQTT_PASSWORD = "things2023"
var MQTT_CLIENT_ID = "access-service"

func init() {
	if val, ok := os.LookupEnv("MQTT_BROKER"); ok {
		MQTT_BROKER = val
	}
	if val, ok := os.LookupEnv("MQTT_USER"); ok {
		MQTT_USER = val
	}
	if val, ok := os.LookupEnv("MQTT_PASSWORD"); ok {
		MQTT_PASSWORD = val
	}
	if val, ok := os.LookupEnv("MQTT_CLIENT_ID"); ok {
		MQTT_CLIENT_ID = val
	}

}
