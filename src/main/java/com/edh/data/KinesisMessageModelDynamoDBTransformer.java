package com.edh.data;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.BasicJsonTransformer;
import com.amazonaws.services.kinesis.connectors.dynamodb.DynamoDBTransformer;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KinesisMessageModelDynamoDBTransformer extends
    BasicJsonTransformer<KinesisMessageModel, Map<String, AttributeValue>> implements
    DynamoDBTransformer<KinesisMessageModel> {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  static {
    MAPPER.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
  }


  public KinesisMessageModelDynamoDBTransformer() {
    super(KinesisMessageModel.class);
  }

  @Override
  public Map<String, AttributeValue> fromClass(KinesisMessageModel message) {
//    Item item = new Item();
//    item.withString("message_id", message.getMessageId())
//        .withString("name", message.getName())
//        .withLong("time", message.getTime())
//        .withJSON("payload", message.getPayload());


    Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
    putStringIfNonempty(item, "name", message.getName());
    item.put("time", new AttributeValue().withN("" + message.getTime()));
    //putLongIfNonempty(item, "time", message.getTime());
    putStringIfNonempty(item, "message_id", message.getMessageId());
    item.put("payload", new AttributeValue().withS("hello"));
    //item.put("payload", new AttributeValue().withM((Map) message.getPayload()));
    return item;
  }

  private void putStringIfNonempty(Map<String, AttributeValue> item, String key, String value) {
    if (value != null && !value.isEmpty()) {
      item.put(key, new AttributeValue().withS(value));
    }
  }

  private void putLongIfNonempty(Map<String, AttributeValue> item, String key, Long value) {
    putStringIfNonempty(item, key, Long.toString(value));
  }

  @Override
  public KinesisMessageModel toClass(Record record) throws IOException {
    KinesisMessageModel model = MAPPER.readValue(record.getData().array(), KinesisMessageModel.class);
    model.setMessageId(record.getSequenceNumber());
    return model;
  }
}
