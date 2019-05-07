package com.ak.pubsub.poc.codec.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ak.pubsub.poc.pojo.Ticker;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;



/**
 * Created by appu_kumar on 4/23/2019.
 */



public class TickerSerializer implements Serializer<Ticker> {

    private static final Logger LOG = LogManager.getLogger(TickerSerializer.class);
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Ticker data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception exception) {
            LOG.error("Error in serializing object" + data);
        }
        return retVal;
    }

    @Override
    public void close() {

    }

}