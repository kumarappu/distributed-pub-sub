package com.ak.pubsub.poc.kafka.producer;

import com.ak.pubsub.poc.proto.domain.CommonProto;
import com.ak.pubsub.poc.codec.serializer.InstrumentProtoSerializer;
import com.ak.pubsub.poc.codec.serializer.TickerSerializer;
import com.ak.pubsub.poc.constants.IKafkaConstants;
import com.ak.pubsub.poc.pojo.Ticker;
import com.ak.pubsub.poc.utils.ConfigUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by appu_kumar on 4/23/2019.
 */
public class ProducerCreator {

    static Properties props;
    static {
        try {
            props = ConfigUtils.getConfiguration("producer-config");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static Producer<Long, byte[]> createByteProducer() {
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }


    public static Producer<Long, Ticker> createTickerProducer() {
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TickerSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static Producer<Long, String> createStringProducer() {
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);

    }


    public static Producer<Long,CommonProto.Instrument > createProtoProducer() {
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, InstrumentProtoSerializer.class.getName());
        return new KafkaProducer<>(props);
    }




}