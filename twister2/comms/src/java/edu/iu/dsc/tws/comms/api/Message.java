package edu.iu.dsc.tws.comms.api;

public final class Message {
  private MessageHeader header;

  private Object payload;

  private Object key;

  private MessageType type;

  private MessageType keyType;

  /**
   * Create a message with Object type
   */
  public Message() {
    this(MessageType.OBJECT);
  }

  private Message(MessageType type) {
    this.type = type;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(MessageType type) {
    return new Builder(type);
  }

  public MessageHeader getHeader() {
    return header;
  }

  public Object getPayload() {
    return payload;
  }

  public MessageType getType() {
    return type;
  }

  public static final class Builder {
    private Message message;

    private Builder() {
      message = new Message();
    }

    private Builder(MessageType type) {
      message = new Message(type);
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

    public Message build() {
      return message;
    }
  }
}
