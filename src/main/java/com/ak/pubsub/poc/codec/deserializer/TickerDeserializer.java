package com.ak.pubsub.poc.codec.deserializer;

/**
 * Created by appu_kumar on 4/23/2019.
 */
import java.util.Map;

import com.ak.pubsub.poc.pojo.Ticker;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TickerDeserializer implements Deserializer<Ticker> {
    private static final Logger LOG = LogManager.getLogger(TickerDeserializer.class);
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Ticker deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        Ticker object = null;
        try {
            object = mapper.readValue(data, Ticker.class);
        } catch (Exception exception) {
            LOG.error("Error in deserializing bytes " + exception);
        }
        return object;
    }

    @Override
    public void close() {
    }
}
