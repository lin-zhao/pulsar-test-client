package io.streamnative.platform;

import org.apache.pulsar.client.api.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class PulsarConsumer {
    public static void main(String[] args) throws IOException {
        //PulsarClient.builder().ioThreads()
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();


        int receiveCount = 0;

        Consumer<String> consumer = client.newConsumer(Schema.STRING).topic("BIG_TOPIC").receiverQueueSize(10)
                .subscriptionType(SubscriptionType.Key_Shared).keySharedPolicy(KeySharedPolicy.autoSplitHashRange().setAllowOutOfOrderDelivery(false))
//                .subscriptionName("Wulala").subscribe();
                .subscriptionName("def").subscribe();

//        Consumer<String> avroConsumer = client.newConsumer(Schema.STRING).topic("USERS_TOPIC")
 //               .subscriptionName("Wulala").subscribe();


        while(true) {
            Message<String> msg = consumer.receive();
            String messageStr = msg.getValue();
            receiveCount ++;
            consumer.acknowledge(msg);
            /*
            if (receiveCount %5 == 4) {
                System.out.println("Redeliver " + messageStr + " later.");
                consumer.reconsumeLater(msg, 5000L , TimeUnit.MILLISECONDS );
            }
            */
            System.out.println(messageStr);
        }

    }
}
