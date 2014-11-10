package com.edh.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonRootName;

import java.io.Serializable;

/**
 * Created by soloman.weng on 4/11/14.
 */
@JsonRootName(value = "message_json")
@JsonIgnoreProperties(ignoreUnknown = true)
public class KinesisMessageModel implements Serializable {
  private String messageId;
  private String name;
  private long time;
  private Object payload;

  public long getTime() {
    return time;
  }

  public Object getPayload() {
    return payload;
  }

  public String getName() {
    return name;
  }

  public String getMessageId() {
    return messageId;
  }

  public void setMessageId(String messageId) {
    this.messageId = messageId;
  }
}
