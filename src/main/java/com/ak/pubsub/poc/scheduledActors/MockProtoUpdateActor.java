package com.spgmi.rtservices.poc.scheduledActors;

import akka.actor.AbstractActor;
import com.spcapitaliq.realtime.common.proto.domain.CommonProto;
import com.spgmi.rtservices.poc.App;
import com.spgmi.rtservices.poc.codec.serializer.InstrumentProtoSerializer;
import com.spgmi.rtservices.poc.codec.serializer.TickerSerializer;
import com.spgmi.rtservices.poc.kafka.producer.ProducerCreator;
import com.spgmi.rtservices.poc.pojo.Ticker;
import com.spgmi.rtservices.poc.utils.AppUtil;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by appu_kumar on 5/2/2019.
 */
public class MockProtoUpdateActor extends AbstractActor {
    private static final Logger LOG = LogManager.getLogger(MockProtoUpdateActor.class);
    Producer<Long, CommonProto.Instrument> producer = ProducerCreator.createProtoProducer();
    AppUtil appUtil=new AppUtil();
    String topics=(String) App.properties.get("topics.name");

    @Override
    public Receive createReceive() {
        return receiveBuilder().matchAny(m->{
            Long key= Long.valueOf ((int)m);

           //Generate a mock instrument update
            CommonProto.Instrument instrument=appUtil.createMockUpdate("Ticker " + m);

            final ProducerRecord<Long, CommonProto.Instrument> record = new ProducerRecord<Long, CommonProto.Instrument>(topics,key,instrument);


            producer.send(record);

           //  LOG.debug("Record sent with key " + m + " to partition :"+instrument);// + metadata.partition() + " with offset " + metadata.offset());


        })
                .build();
    }
}