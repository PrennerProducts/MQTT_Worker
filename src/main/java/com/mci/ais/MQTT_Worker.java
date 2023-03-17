package com.mci.ais;


import org.eclipse.paho.client.mqttv3.*;

import javax.security.auth.callback.Callback;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class MQTT_Worker implements MqttCallback {

    private MqttClient mqtt;
    private String workerId = UUID.randomUUID().toString();
    private boolean shutdownflag = false;

    MQTT_Worker worker;

    public MQTT_Worker(String mqtturl) throws MqttException {
        mqtt = new MqttClient(mqtturl, workerId);
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.err.println("Worker lost the connection to MQTT-Broker!");
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println("New message!");
        String [] topicparts = topic.split("/");
        if (topicparts[1].equals(workerId)){
            System.out.println("Topicparts[1] = " + topicparts[1]);
            // message signature=  [OK/NOK]: [darts, null]
            String [] msgparts = message.toString().split(":");
            if(msgparts[0].equals("OK")){
                System.out.println("Worker gets OK stauts");
                if (msgparts.length == 2){
                    long darts = Long.parseLong(msgparts[1]);
                    System.out.println("Worker " + workerId + " recived " + msgparts[1] + " darts");
                    // Throw darts and send MQTT message with the result to the coordinator
                    throwDartsAndSendMessage(darts);
                }

            } else if (msgparts[0].equals("NOK")) {
                System.out.println("Worker gets Status NOK! no more Darts to throw");
                shutdownflag = true;
            }else {
                System.err.println("ERROR: unknown message-type");
            }
        }
        else if (topic.equals("worker/broadcast")){
            System.out.println("Broadcast arrived!");
            // Nachricht an alle Worker
        }else {
            System.err.println("ERROR: no valid Message arrived!");
            System.err.println(message.toString());
        }

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // dont need!
    }

    public void connect() throws MqttException {
        mqtt.connect();
        mqtt.setCallback(this);
        mqtt.subscribe("worker/" + workerId);
        System.out.println("Subscripre to: worker/" + workerId);
        mqtt.subscribe("worker/broadcast");
        System.out.println("Worker " + workerId + " connected");
    }

    public void disconnect() throws  MqttException{
        mqtt.disconnect();
        mqtt.close();
        System.out.println("Worker " + workerId + " disconnected");
    }

    public void work() throws InterruptedException, MqttException {
        while (!shutdownflag){
            MqttMessage message = new MqttMessage();
            mqtt.publish("coordinator/request/" + workerId, message );
            Thread.sleep(500);
        }
        disconnect();
    }


    //Throws darts and sends a message with the hits to the Coordinator
    private void throwDartsAndSendMessage(long darts) throws MqttException {
        // throw darts
        long hits = 0;
        for (long i = 0; i < darts; i++){
            double x = ThreadLocalRandom.current().nextDouble(-1.0, 1.0);
            double y = ThreadLocalRandom.current().nextDouble(-1.0, 1.0);
            if(x*x + y*y <= 1.0){
                hits++;
            }
        }
        // send message
        String msg;
        msg = String.valueOf(darts) + ":" +  String.valueOf(hits) ;
        MqttMessage sendMessage = new MqttMessage(msg.getBytes());
        mqtt.publish("coordinator/result", sendMessage);
        System.out.println("Worker sends result to Coordinator! Result: " + msg);
    }






    public static void main(String[] args) throws MqttException, InterruptedException {
        MQTT_Worker worker1 = new MQTT_Worker("tcp://localhost:1883");
        worker1.connect();
        worker1.work();
        worker1.disconnect();

    }

}