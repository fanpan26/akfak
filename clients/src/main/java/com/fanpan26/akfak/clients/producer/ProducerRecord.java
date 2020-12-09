package com.fanpan26.akfak.clients.producer;

/**
 * @author fanyuepan
 */
public final class ProducerRecord<K,V> {

    private final String topic;
    private final Integer partition;
    private final K key;
    private final V value;
    private final Long timestamp;

    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic cannot be null");
        }
        if (timestamp != null && timestamp < 0) {
            throw new IllegalArgumentException("Invalid timestamp " + timestamp);
        }
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this(topic, partition, null, key, value);
    }

    public ProducerRecord(String topic, V value) {
        this(topic, null, null, null, value);
    }

    public String topic() {
        return topic;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    public Long timestamp() {
        return timestamp;
    }

    public Integer partition() {
        return partition;
    }

    @Override
    public String toString() {
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "ProducerRecord(topic=" + topic + ", partition=" + partition + ", key=" + key + ", value=" + value +
                ", timestamp=" + timestamp + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof ProducerRecord)) {
            return false;
        }

        ProducerRecord<?, ?> that = (ProducerRecord<?, ?>) o;

        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        } else if (partition != null ? !partition.equals(that.partition) : that.partition != null) {
            return false;
        } else if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
            return false;
        } else if (value != null ? !value.equals(that.value) : that.value != null) {
            return false;
        } else if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
