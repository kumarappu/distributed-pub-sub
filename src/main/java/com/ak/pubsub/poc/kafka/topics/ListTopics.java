package com.ak.pubsub.poc.kafka.topics;

/**
 * Created by appu_kumar on 4/22/2019.
 */
import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.ak.pubsub.poc.utils.ConfigUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

/**
 * Lists All the topic in current Kafka configuration.
 * @author ninad
 *
 */
public class ListTopics {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Properties prop = ConfigUtils.getConfiguration("admin-config");
        AdminClient admin = AdminClient.create(prop);
        ListTopicsResult topics = admin.listTopics();
        Set<String> topicNames = topics.names().get();
        for(String topic: topicNames) {
            System.out.println(topic);
        }
    }

}