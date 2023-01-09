import Kafka from "node-rdkafka";

console.log(Kafka.features);
console.log(Kafka.librdkafkaVersion);

const producer = new Kafka.Producer({
  "metadata.broker.list": "localhost:9093",
});

producer.connect();
producer.on("ready", () => {
  try {
    const topic = "input-topic";
    const partition = null;
    const message = Buffer.from("world");
    const key = "hello";
    const timestamp = Date.now();
    producer.produce(topic, partition, message, key, timestamp);
  } catch (error) {
    console.log(error);
  }
});

producer.on("event.error", function (error) {
  console.error("error from producer");
  console.error(err);
});

producer.setPollInterval(100);
