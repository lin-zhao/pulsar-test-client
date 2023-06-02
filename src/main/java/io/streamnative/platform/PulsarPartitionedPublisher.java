package io.streamnative.platform;


import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
public class PulsarPartitionedPublisher {

    public static void main(String[] args) throws IOException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();


        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            Producer<User> userPublisher = client.newProducer(Schema.AVRO(User.class)).topic("USERS_TOPIC_PART")
                    .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                    .enableBatching(true)
                    .batchingMaxPublishDelay(3, TimeUnit.SECONDS)
                    .batchingMaxMessages(500)
                    .create();


            for (int i = 0; i < 20000; i ++) {
                System.out.println("======= Publishing new user message " + i);
                User newUser = new User("user-" + i, System.currentTimeMillis());
                MessageId avroId = userPublisher.send(newUser);
                System.out.println("======= Finished publish avro message, message id is " + new String(avroId.toByteArray())
                        + "==============");
            }

        } catch (Exception e){
            System.out.println("======= Exception thrown ==============");
            e.printStackTrace();
            throw e;
        }
    }
}
