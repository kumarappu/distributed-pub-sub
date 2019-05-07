package com.ak.pubsub.poc.codec.serializer;

import com.ak.pubsub.poc.proto.domain.CommonProto;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Created by appu_kumar on 5/2/2019.
 */
public class InstrumentProtoSerializer implements Serializer<CommonProto.Instrument> {
    private static final Logger LOG = LogManager.getLogger(InstrumentProtoSerializer.class);
    @Override
    public byte[] serialize(final String topic, final CommonProto.Instrument instrument) {
        byte[] retVal = null;
try{
    retVal=instrument.toByteArray();
    } catch (Exception exception) {
        LOG.error("Error in serializing Instrument" + instrument);
    }
        return retVal;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }
}
