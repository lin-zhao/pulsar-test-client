package io.streamnative.platform;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class PulsarBatchedPublisher {
    public static void main(String[] args) throws IOException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();


        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            Producer<User> userPublisher = client.newProducer(Schema.AVRO(User.class)).topic("USERS_TOPIC_BATCHED")
                    .enableBatching(true)
                    .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
                    .batchingMaxBytes(130000)
                    .create();


            for (int i = 0; i < 21; i ++) {
                System.out.println("======= Publishing new user message " + i);
                User newUser = new User("user-" + i, System.currentTimeMillis());
                userPublisher.sendAsync(newUser);
            }

            userPublisher.flush();
/*
            String line = br.readLine();
            while (line != null) {
                if (!line.equals("")) {
                    try {
                        System.out.println("======= Publishing new string message ==============");
                        MessageId id = publisher.send(line);
                        System.out.println("======= Finished publish string message, message id is " + new String(id.toByteArray())
                                + "==============");

                        System.out.println("======= Publishing new avro message ==============");
                        User newUser = new User(line, System.currentTimeMillis());
                        MessageId avroId = userPublisher.send(newUser);
                        System.out.println("======= Finished publish avro message, message id is " + new String(avroId.toByteArray())
                                + "==============");

                    } catch (PulsarClientException e) {
                        System.out.println("========= Publish failed ===========");
                        e.printStackTrace();
                    }
                }
                line = br.readLine();
            }

 */
        } catch (Exception e){
            System.out.println("======= Exception thrown ==============");
            e.printStackTrace();
            throw e;
        }
    }
}
