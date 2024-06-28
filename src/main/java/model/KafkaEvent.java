package model;

public class KafkaEvent {
    private String senderName;
    private String receiverName;
    private String message;
    private long timestamp;
    private boolean sensitive; // if the message contain sensitive words

    public KafkaEvent() {
    }

    public KafkaEvent(String senderName, String receiverName, String message, long timestamp) {
        this.senderName = senderName;
        this.receiverName = receiverName;
        this.message = message;
        this.timestamp = timestamp;
    }

    // Getters and setters
    public String getSenderName() {
        return senderName;
    }

    public void setSenderName(String senderName) {
        this.senderName = senderName;
    }

    public String getReceiverName() {
        return receiverName;
    }

    public void setReceiverName(String receiverName) {
        this.receiverName = receiverName;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isSensitive() {
        return sensitive;
    }

    public void setSensitive(boolean sensitive) {
        this.sensitive = sensitive;
    }

    @Override
    public String toString() {
        return "KafkaEvent{" +
                "senderName='" + senderName + '\'' +
                ", receiverName='" + receiverName + '\'' +
                ", message='" + message + '\'' +
                ", timestamp=" + timestamp +
                ", sensitive=" + sensitive +
                '}';
    }
}

