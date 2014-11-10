package com.edh.data;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.dynamodb.DynamoDBEmitter;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;

public class DynamoDBMessageModelPipeline implements
    IKinesisConnectorPipeline<KinesisMessageModel, Map<String, AttributeValue>> {

  @Override
  public IEmitter<Map<String, AttributeValue>> getEmitter(KinesisConnectorConfiguration configuration) {
    return new DynamoDBEmitter(configuration);
  }

  @Override
  public IBuffer<KinesisMessageModel> getBuffer(KinesisConnectorConfiguration configuration) {
    return new BasicMemoryBuffer<KinesisMessageModel>(configuration);
  }

  @Override
  public ITransformer<KinesisMessageModel, Map<String, AttributeValue>>
  getTransformer(KinesisConnectorConfiguration configuration) {
    return new KinesisMessageModelDynamoDBTransformer();
  }

  @Override
  public IFilter<KinesisMessageModel> getFilter(KinesisConnectorConfiguration configuration) {
    return new AllPassFilter<KinesisMessageModel>();
  }
}
