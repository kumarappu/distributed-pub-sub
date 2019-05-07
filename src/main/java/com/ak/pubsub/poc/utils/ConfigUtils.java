package com.ak.pubsub.poc.utils;

/**
 * Created by appu_kumar on 4/22/2019.
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtils {

    public static Properties getConfiguration(String classpathFileName) throws IOException {


        String configDirPath=System.getProperty("user.dir")+File.separator+"config"+ File.separator+classpathFileName+".properties";
        System.out.println("Config File path:"+configDirPath);
        InputStream configStream = new FileInputStream(configDirPath);

        if(configStream==null){
            System.out.println("Reading configuration from classpath");
            configStream = ConfigUtils.class.getClassLoader().getResourceAsStream(classpathFileName+".properties");
        }

        Properties prop = new Properties();
        prop.load(configStream);
        configStream.close();
        return prop;
    }

}