package com.mci.ais;


import org.eclipse.paho.client.mqttv3.*;

import javax.security.auth.callback.Callback;
import java.util.Arrays;
import java.util.UUID;
public class MQTT_Worker implements MqttCallback {

    private MqttClient mqtt;
    MQTT_Worker worker;

    public MQTT_Worker(String mqtturl) throws MqttException {
        String myUUID = UUID.randomUUID().toString();
        mqtt = new MqttClient(mqtturl, myUUID);
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.err.println("Worker lost the connection to MQTT-Broker!");
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // dont need!
    }

    public void connect(){
        mqtt.connect();
        mqtt.setCallback(this);
        mqtt.subscribe("Worker/" + mqtt.myUUID );
    }




    public static void main(String[] args) {
        System.out.println("Hello world!");
    }

}