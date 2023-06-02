package io.streamnative.platform;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.pulsar.client.api.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.pulsar.client.impl.RoundRobinPartitionMessageRouterImpl;

public class PulsarPublisher {
    static byte[] bytesFromLong(Long version) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(version);
        buffer.rewind();
        return buffer.array();
    }

    static long longFromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }


    public static void main(String[] args) throws IOException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();


        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            Producer<String> publisher = client.newProducer(Schema.STRING).topic("TOPIC-P-TEST")
                   // .producerName(UUID.randomUUID().toString())
                    .messageRouter(new RoundRobinPartitionMessageRouterImpl(HashingScheme.JavaStringHash, 0, false, 10))
                    .create();

//            Producer<User> userPublisher = client.newProducer(Schema.AVRO(User.class)).topic("USERS_TOPIC").create();


                String line = br.readLine();
                while (line != null) {
                    if (!line.equals("")) {
                        try {
                            System.out.println("======= Publishing new string message ==============");
                            MessageId id = publisher.send(line);
                            System.out.println("======= Finished publish string message, message id is " + new String(id.toByteArray())
                                    + "==============");

                            System.out.println("======= Publishing new avro message ==============");
 //                           User newUser = new User(line, System.currentTimeMillis());
  //                          MessageId avroId = userPublisher.send(newUser);
 //                           System.out.println("======= Finished publish avro message, message id is " + new String(avroId.toByteArray())
  //                                  + "==============");

                        } catch (PulsarClientException e) {
                            System.out.println("========= Publish failed ===========");
                            e.printStackTrace();
                        }
                    }
                    line = br.readLine();
                }
        } catch (Exception e){
            System.out.println("======= Exception thrown ==============");
            e.printStackTrace();
            throw e;
        }
    }
}
