package com.ak.pubsub.poc.kafka.consumer;

import com.ak.pubsub.poc.proto.domain.CommonProto;
import com.ak.pubsub.poc.codec.deserializer.InstrumentProtoDeserializer;
import com.ak.pubsub.poc.codec.deserializer.TickerDeserializer;
import com.ak.pubsub.poc.constants.IKafkaConstants;
import com.ak.pubsub.poc.pojo.Ticker;
import com.ak.pubsub.poc.utils.ConfigUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.*;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by appu_kumar on 4/23/2019.
 */
public class ConsumerCreator {

    static Properties props;
    static {
        try {
            props = ConfigUtils.getConfiguration("consumer-config");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Consumer<Long, Ticker> createTickerConsumer() {
       return  createTickerConsumer(IKafkaConstants.TOPIC_NAME);
    }

    public static Consumer<Long, Ticker> createTickerConsumer(String topics) {

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TickerDeserializer.class.getName());
        final Consumer<Long, Ticker> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topics));
        return consumer;
    }

    public static Consumer<Long, CommonProto.Instrument> createProtoConsumer(String topics) {

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, InstrumentProtoDeserializer.class.getName());
        final Consumer<Long, CommonProto.Instrument> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topics));
        return consumer;
    }



    public static Consumer<Long, byte[]> createByteArrayConsumer() {

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        final Consumer<Long,  byte[]> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(IKafkaConstants.TOPIC_NAME));
        return consumer;
    }

    public static Consumer<Long, String> createStringConsumer() {

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final Consumer<Long,  String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(IKafkaConstants.TOPIC_NAME));
        return consumer;
    }



}
