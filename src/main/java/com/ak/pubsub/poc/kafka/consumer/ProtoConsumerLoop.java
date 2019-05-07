package com.ak.pubsub.poc.kafka.consumer;

import com.ak.pubsub.poc.App;
import com.ak.pubsub.poc.proto.domain.CommonProto;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by appu_kumar on 4/30/2019.
 */
public class ProtoConsumerLoop implements Runnable {

    private static final Logger LOG = LogManager.getLogger(ProtoConsumerLoop.class);
    Boolean isLoggingEnabled=   Boolean.parseBoolean((String) App.properties.get("enable.logging"));
    String tickerStartRange=(String)App.properties.get("ticker.start.range");
    private final Consumer<Long, CommonProto.Instrument> consumer;
    private final int id;

    public ProtoConsumerLoop(int id,
                             String topics) {

        this.id = id;
        consumer = ConsumerCreator.createProtoConsumer(topics);

    }

    @Override
    public void run() {
        try {

            System.out.println("Starting up proto consumer loop "+this.id);
            while (true) {
                final ConsumerRecords<Long, CommonProto.Instrument> consumerRecords = consumer.poll(Long.MAX_VALUE);
                if(isLoggingEnabled) {

                    consumerRecords.forEach(record -> {
                      //Only print logs for 1 ticker
                       if(record.value().getIdentifier().equals("Ticker "+tickerStartRange)) {

                            long latency = System.currentTimeMillis() -record.value().getFields(0).getFieldValue(0).getInt64Val();
                        Map<String, Object> data = new HashMap<>();
                            data.put("partition", record.partition());
                            data.put("offset", record.offset());
                            data.put("value", record.value().getIdentifier());
                            data.put("latency", latency);


                            LOG.info(this.id + ":" + data);
                       }
                    });
                }
            }

        } catch (WakeupException e) {
            // ignore for shutdown
        }
        finally {
            consumer.close();
        }

    }
    public void shutdown() {
        consumer.wakeup();
    }

}
