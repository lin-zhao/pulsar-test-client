package io.streamnative.platform;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.internal.DefaultImplementation;

public class PulsarAvroConsumer {
    public static void main(String[] args) throws IOException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        int receiveCount = 0;

        Schema<User> schema = DefaultImplementation.getDefaultImplementation().newAvroSchema(SchemaDefinition.builder().withAlwaysAllowNull(false).withPojo(User.class).build());
        //Schema<User> schema = Schema.AVRO(User.class);
        Consumer<User> avroConsumer = client.newConsumer(schema).topic("USERS_TOPIC")
        //Consumer<User> avroConsumer = client.newConsumer(Schema.AVRO(User.class)).topic("USERS_TOPIC")
                .subscriptionName("Wulala").subscribe();

        for (int i =0; i < 40; i ++) {
            Message<User> msg = avroConsumer.receive();
            String messageStr = msg.toString();
            receiveCount ++;
        }

        while(true) {
            Message<User> msg = avroConsumer.receive();
            String messageStr = msg.toString();
            receiveCount ++;
            avroConsumer.acknowledge(msg);
            /*
            if (receiveCount %5 == 4) {
                System.out.println("Redeliver " + messageStr + " later.");
                consumer.reconsumeLater(msg, 5000L , TimeUnit.MILLISECONDS );
            }
            */
            System.out.println("received " + messageStr);
        }

    }
}
