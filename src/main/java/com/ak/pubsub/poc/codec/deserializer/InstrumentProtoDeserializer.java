package com.ak.pubsub.poc.codec.deserializer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ak.pubsub.poc.proto.domain.CommonProto;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Created by appu_kumar on 5/2/2019.
 */
public class InstrumentProtoDeserializer implements Deserializer<CommonProto.Instrument> {

    private static final Logger LOG = LogManager.getLogger(InstrumentProtoDeserializer.class);
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public CommonProto.Instrument deserialize(String topic, byte[] data) {

        try {
            return CommonProto.Instrument.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            LOG.error("Received unparseable message", e);
            throw new RuntimeException("Received unparseable message " + e.getMessage(), e);
        }
    }



    @Override
    public void close() {
    }

}
