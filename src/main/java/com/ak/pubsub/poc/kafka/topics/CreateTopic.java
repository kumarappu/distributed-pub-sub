package com.ak.pubsub.poc.kafka.topics;

/**
 * Created by appu_kumar on 4/22/2019.
 */
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.ak.pubsub.poc.utils.ConfigUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;


public class CreateTopic {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        Properties properties = ConfigUtils.getConfiguration("admin-config");
        AdminClient adminClient = AdminClient.create(properties);
        CreateTopicsResult result = adminClient.createTopics(Arrays.asList(new NewTopic("test100", 1, (short)1)));

        System.out.println("Topic Created: "+    result.values());


        // Other way to create topic
		/*ZkClient zkClient = ZkUtils.createZkClient("localhost:2181", 100, 10000);
		AdminUtils.createTopic(ZkUtils.apply(zkClient, JaasUtils.isZkSecurityEnabled()), "test", 1, 1, new Properties(), null);*/
    }

}