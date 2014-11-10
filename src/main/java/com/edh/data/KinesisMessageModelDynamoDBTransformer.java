package com.edh.data;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.BasicJsonTransformer;
import com.amazonaws.services.kinesis.connectors.dynamodb.DynamoDBTransformer;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KinesisMessageModelDynamoDBTransformer extends
    BasicJsonTransformer<KinesisMessageModel, Map<String, AttributeValue>> implements
    DynamoDBTransformer<KinesisMessageModel> {

  private static final ObjectMapper MAPPER = new ObjectMapper();


  public KinesisMessageModelDynamoDBTransformer() {
    super(KinesisMessageModel.class);
  }

  @Override
  public Map<String, AttributeValue> fromClass(KinesisMessageModel message) {
    Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
    item.put("message_id", new AttributeValue().withS(message.getMessageId()));
    item.put("name", new AttributeValue().withS(message.getName()));
    item.put("time", new AttributeValue().withN("" + message.getTime()));
    item.put("payload", new AttributeValue().withM(convertJsonPayload((Map<String, Object>) message.getPayload())));
    return item;
  }

  @Override
  public KinesisMessageModel toClass(Record record) throws IOException {
    KinesisMessageModel model = MAPPER.readValue(record.getData().array(), KinesisMessageModel.class);
    model.setMessageId(record.getSequenceNumber());
    return model;
  }

  private static Map<String, AttributeValue> convertJsonPayload(Map<String, Object> payload) {
    Map<String, AttributeValue> target = new HashMap<String, AttributeValue>();
    for(Map.Entry<String, Object> e : payload.entrySet()) {
      AttributeValue value = toAttributeValue(e.getValue());
      if(value != null) {
        target.put(e.getKey(), value);
      }
    }
    return target;
  }

  private static AttributeValue toAttributeValue(Object value){
    if(value instanceof String) {
      if(!((String)value).isEmpty()) {
        return new AttributeValue().withS((String) value);
      }
    }else if(value instanceof Boolean) {
      return new AttributeValue().withBOOL((Boolean)value);
    }else if(value instanceof Number) {
      return new AttributeValue().withN(value.toString());
    }else if(value instanceof Map) {
      Map<String, AttributeValue> mapVal = convertJsonPayload((Map<String, Object>)value);
      if(!mapVal.isEmpty()) {
        return new AttributeValue().withM(convertJsonPayload((Map<String, Object>) value));
      }
    }else if(value instanceof Collection) {
      Collection<AttributeValue> list = new ArrayList<AttributeValue>();
      for(Object val : (Collection)value) {
        AttributeValue attrVal = toAttributeValue(val);
        if(attrVal != null) {
          list.add(toAttributeValue(val));
        }
      }
      return new AttributeValue().withL(list);
    }
    return null;
  }
}
