package io.streamnative.platform;

import org.apache.pulsar.client.api.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class KeyShareConsumer {
    public static void main(String[] args) throws IOException, InterruptedException {
        //args[0] is number of messages to receive
        //PulsarClient.builder().ioThreads()
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();


        int totalToAck = Integer.parseInt(args[0]);
        int receiveCount = 0;

        Consumer<String> consumer = client.newConsumer(Schema.STRING).topic("KEY_BIG_TOPIC").receiverQueueSize(10)
                .subscriptionType(SubscriptionType.Key_Shared)
                        .subscriptionName("def").subscribe();
//                .subscriptionName("Wulala").subscribe();

//        Consumer<String> avroConsumer = client.newConsumer(Schema.STRING).topic("USERS_TOPIC")
        //               .subscriptionName("Wulala").subscribe();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));



        while(true) {
            System.out.println("Hit enter to ack " + totalToAck + " more messages.");
            br.readLine();
            receiveCount = 0;
            while (receiveCount < totalToAck) {
                Message<String> msg = consumer.receive();
                String messageStr = msg.getValue();
                receiveCount++;
                consumer.acknowledge(msg);
                System.out.println(consumer.getConsumerName() + " received and acked key=" + msg.getKey() + " msg=" + msg.getValue());
            /*
            if (receiveCount %5 == 4) {
                System.out.println("Redeliver " + messageStr + " later.");
                consumer.reconsumeLater(msg, 5000L , TimeUnit.MILLISECONDS );
            }
            */
                System.out.println(messageStr);
            }

            System.out.println("Acked " + totalToAck + " messages.");
        }
    }
}
