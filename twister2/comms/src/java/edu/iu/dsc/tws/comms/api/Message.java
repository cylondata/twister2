package edu.iu.dsc.tws.comms.api;

public final class Message {
  private MessageHeader header;

  private Object payload;

  private Message() {
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public MessageHeader getHeader() {
    return header;
  }

  public Object getPayload() {
    return payload;
  }

  public static final class Builder {
    private Message message;

    private Builder() {
      message = new Message();
    }

    public Builder reInit() {
      message.header = null;
      message.payload = null;
      return this;
    }

    public Builder setHeader(MessageHeader header) {
      message.header = header;
      return this;
    }

    public Builder setPayload(Object payload) {
      message.payload = payload;
      return this;
    }

    private Message build() {
      return message;
    }
  }
}
