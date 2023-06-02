package io.streamnative.platform;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.RoundRobinPartitionMessageRouterImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;


public class KeySharePublisher {
    public static void main(String[] args) throws IOException {
        // args[0]: kys. args[1], number of messages for the key


        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();


        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            Producer<String> publisher = client.newProducer(Schema.STRING).topic("KEY_BIG_TOPIC")
                    // .producerName(UUID.randomUUID().toString())
                    .create();

//            Producer<User> userPublisher = client.newProducer(Schema.AVRO(User.class)).topic("USERS_TOPIC").create();


            int published = 0;
            System.out.print("Enter 'k numberMessages':");
            String line = br.readLine();

            Map<String, Integer> keyCountMap = new HashMap<>();

            while (line != null ) {
                String[] a = line.split(" ");
                String key = a[0];
                int numberMessages = Integer.parseInt(a[1]);
                published = 0;
                if (!keyCountMap.containsKey(key)) {
                    keyCountMap.put(key, 0);
                }

                while (published < numberMessages) {
                    try {
                        int msgNum = keyCountMap.get(key) + 1;
                        System.out.println("======= Publishing new string message no " + msgNum + " for key " + key);
                        MessageId id = publisher.newMessage().key(key).value("message-" + msgNum).send();
                        System.out.println("======= Finished publish string message"
                                + "==============");
                        published++;
                        keyCountMap.put(key, msgNum);

                    } catch (PulsarClientException e) {
                        System.out.println("========= Publish failed ===========");
                        e.printStackTrace();
                    }
                }
                System.out.print("Enter 'k numberMessages':");
                line = br.readLine();
            }
        } catch (Exception e){
            System.out.println("======= Exception thrown ==============");
            e.printStackTrace();
            throw e;
        }
    }
}
