package com.edh.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DynamoDBExecutor extends KinesisConnectorExecutorBase<KinesisMessageModel, Map<String, AttributeValue>> {
  private static final Log LOG = LogFactory.getLog(DynamoDBExecutor.class);

  private static String configFile = "config.properties";

  private final KinesisConnectorConfiguration config;
  private final Properties properties;

  public DynamoDBExecutor(String configFile) {
    InputStream configStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);

    if (configStream == null) {
      String msg = "Could not find resource " + configFile + " in the classpath";
      throw new IllegalStateException(msg);
    }
    properties = new Properties();
    try {
      properties.load(configStream);
      configStream.close();
    } catch (IOException e) {
      String msg = "Could not load properties file " + configFile + " from classpath";
      throw new IllegalStateException(msg, e);
    }
    config = new KinesisConnectorConfiguration(properties, getAWSCredentialsProvider());
    LOG.info(config);

    // Initialize executor with configurations
    super.initialize(config);
  }

  @Override
  public KinesisConnectorRecordProcessorFactory<KinesisMessageModel, Map<String, AttributeValue>> getKinesisConnectorRecordProcessorFactory() {
    return new KinesisConnectorRecordProcessorFactory<KinesisMessageModel, Map<String, AttributeValue>>(new DynamoDBMessageModelPipeline(), config);
  }

  public AWSCredentialsProvider getAWSCredentialsProvider() {
    return new DefaultAWSCredentialsProviderChain();
  }

  public static void main(String[] args) {
    DynamoDBExecutor dynamoDBExecutor = new DynamoDBExecutor(configFile);
    dynamoDBExecutor.run();
  }
}