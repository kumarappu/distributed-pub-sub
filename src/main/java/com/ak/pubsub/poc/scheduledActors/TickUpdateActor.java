package com.ak.pubsub.poc.scheduledActors;

import akka.actor.AbstractActor;
import com.ak.pubsub.poc.App;
import com.ak.pubsub.poc.pojo.Ticker;
import com.ak.pubsub.poc.kafka.producer.ProducerCreator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.kafka.clients.producer.Producer;

/**
 * Created by appu_kumar on 4/21/2019.
 */

public class TickUpdateActor extends AbstractActor {
    private static final Logger LOG = LogManager.getLogger(TickUpdateActor.class);

    Producer<Long, Ticker> producer = ProducerCreator.createTickerProducer();
    String topics=(String) App.properties.get("topics.name");

    @Override
    public Receive createReceive() {
        return receiveBuilder().matchAny(m->{

/*
                    String message="This is record  for Ticker " + m +" generated at time "+System.currentTimeMillis();
                    final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, message);
*/

                    Ticker ticker=new Ticker("Ticker " + m,+System.currentTimeMillis());//TODO create mock updates here
                    //final ProducerRecord<Long, byte[]> record = new ProducerRecord<Long, byte[]>(IKafkaConstants.TOPIC_NAME, tickerSerializer.serialize(IKafkaConstants.TOPIC_NAME,ticker));
                  Long key= Long.valueOf ((int)m);
                  final ProducerRecord<Long, Ticker> record = new ProducerRecord<Long, Ticker>(topics,key,ticker);


                        producer.send(record);

                       // LOG.info("Record sent with key " + m + " to partition " + metadata.partition() + " with offset " + metadata.offset());


                })
                .build();
    }
}