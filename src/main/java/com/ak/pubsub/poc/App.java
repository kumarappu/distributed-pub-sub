package com.ak.pubsub.poc;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Props;
import com.ak.pubsub.poc.codec.deserializer.TickerDeserializer;
import com.ak.pubsub.poc.constants.IKafkaConstants;
import com.ak.pubsub.poc.kafka.consumer.ConsumerCreator;
import com.ak.pubsub.poc.kafka.consumer.ConsumerLoop;
import com.ak.pubsub.poc.kafka.consumer.ProtoConsumerLoop;
import com.ak.pubsub.poc.pojo.Ticker;
import com.ak.pubsub.poc.scheduledActors.MockProtoUpdateActor;
import com.ak.pubsub.poc.scheduledActors.TickUpdateActor;
import com.ak.pubsub.poc.utils.ConfigUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Created by appu_kumar on 4/21/2019.
 */
public class App {

    private static final Logger LOG = LogManager.getLogger(App.class);

   public static   Properties properties;
    public static void main(String[] args) throws InterruptedException, IOException {
        properties= ConfigUtils.getConfiguration("application");
        properties.list(System.out);


        if(properties.get("application.type").equals("producer")){

            if(properties.get("payload.type").equals("proto")){
                runProtoProducer();
            }else
            {
                runProducer();
            }

     }
     else if(properties.get("application.type").equals("consumer")){
            if(properties.get("payload.type").equals("proto")){
                runProtoConsumer();
            }else
            {
                runTickerConsumer();
            }


     }


    }



    static void runProtoProducer() throws InterruptedException {
        ActorSystem system = ActorSystem.create("Kafka-POC");
        ActorRef protoActor = system.actorOf(Props.create(MockProtoUpdateActor.class, ()->new MockProtoUpdateActor()));

        //This will schedule to send the a mock update
        //to the tickActor after 0ms repeating every 250ms
        int tickerStartRange=Integer.parseInt((String)properties.get("ticker.start.range"));
        int tickerEndRange=Integer.parseInt((String)properties.get("ticker.end.range"));
        int conflationDuration=Integer.parseInt((String)properties.get("conflation.duration"));
        int testDuration=Integer.parseInt((String)properties.get("test.duration.in.seconds"));

        IntStream.range(tickerStartRange,tickerEndRange).forEach(x-> {
            Cancellable cancellable1 = system.scheduler().schedule(Duration.Zero(),
                    Duration.create(conflationDuration, TimeUnit.MILLISECONDS), protoActor, x,
                    system.dispatcher(), null);

        });


        //This cancels further Ticks to be sent
        Thread.sleep(testDuration*1000);
        // cancellable.cancel();
        system.terminate();
    }



    static void runProducer() throws InterruptedException {
        ActorSystem system = ActorSystem.create("Kafka-POC");
        ActorRef tickActor = system.actorOf(Props.create(TickUpdateActor.class, ()->new TickUpdateActor()));

        //This will schedule to send the Tick-message
        //to the tickActor after 0ms repeating every 250ms
        int tickerStartRange=Integer.parseInt((String)properties.get("ticker.start.range"));
        int tickerEndRange=Integer.parseInt((String)properties.get("ticker.end.range"));
        int conflationDuration=Integer.parseInt((String)properties.get("conflation.duration"));
        int testDuration=Integer.parseInt((String)properties.get("test.duration.in.seconds"));

        IntStream.range(tickerStartRange,tickerEndRange).forEach(x-> {
            Cancellable cancellable1 = system.scheduler().schedule(Duration.Zero(),
                    Duration.create(conflationDuration, TimeUnit.MILLISECONDS), tickActor, x,
                    system.dispatcher(), null);

        });


        //This cancels further Ticks to be sent
        Thread.sleep(testDuration*1000);
        // cancellable.cancel();
        system.terminate();
    }



    static void runTickerConsumer(){
        int numConsumers =Integer.parseInt((String) properties.get("consumers.size"));
        String topics=(String)properties.get("topics.name");
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        final List<ConsumerLoop> consumers = new ArrayList<>();

        for (int i = 0; i < numConsumers; i++) {
            ConsumerLoop consumer = new ConsumerLoop(i, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ConsumerLoop consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                  LOG.error(e);
                }
            }
        });
    }

    static void runProtoConsumer(){
        int numConsumers =Integer.parseInt((String) properties.get("consumers.size"));
        String topics=(String)properties.get("topics.name");
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        final List<ProtoConsumerLoop> consumers = new ArrayList<>();

        for (int i = 0; i < numConsumers; i++) {
            ProtoConsumerLoop consumer = new ProtoConsumerLoop(i, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ProtoConsumerLoop consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.error(e);
                }
            }
        });
    }

    static void runConsumer(){
        System.out.println("Start running Kafka consumer");
        // Consumer<Long, byte[]> consumer = ConsumerCreator.createByteArrayConsumer();
        Consumer<Long, String> consumer = ConsumerCreator.createStringConsumer();


        int noMessageToFetch = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }

            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            consumer.commitAsync();
        }
        System.out.println("Stopping  Kafka consumer");
        consumer.close();


    }

    static void runByteArrayConsumer(){
        System.out.println("Start running Kafka consumer");
         Consumer<Long, byte[]> consumer = ConsumerCreator.createByteArrayConsumer();

        TickerDeserializer tickerDeserializer=new TickerDeserializer();

        int noMessageToFetch = 0;

        while (true) {
            final ConsumerRecords<Long, byte[]> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }

            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                Ticker tickUpdate=tickerDeserializer.deserialize(IKafkaConstants.TOPIC_NAME,record.value());
                System.out.println("Record value " + tickUpdate);
                System.out.println("Record value 2" + record.toString());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            consumer.commitAsync();
        }
        System.out.println("Stopping  Kafka consumer");
        consumer.close();


    }

}
