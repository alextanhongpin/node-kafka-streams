import { KafkaStreams } from "kafka-streams";
import config from "./config.js";

const kStream = new KafkaStreams(config);
kStream.on("error", console.error);

// https://github.com/nodefluent/kafka-streams/blob/master/examples/wordCount.js
const stream = kStream.getKStream();
stream
  .from("input-topic")
  .mapJSONConvenience()
  .map((msg) => {
    console.log("msg", msg);
    const words = msg.value.split(" ");
    const word = words[Math.floor(Math.random() * words.length)];
    return {
      someField: word,
    };
  })
  .countByKey("someField", "count")
  .filter((kv) => kv.count >= 3)
  .map((kv) => kv.someField + " " + kv.count)
  .tap((kv) => console.log(kv))
  .forEach(console.log);

const inputStream = kStream.getKStream();
inputStream.to("input-topic");
const produceInterval = setInterval(() => {
  inputStream.writeToStream("this is a random choice of words");
}, 1000);

Promise.all([stream.start(), inputStream.start()]).then(() => {
  setTimeout(() => {
    kStream.closeAll();
    clearInterval(produceInterval);
    console.log("stopped");
  }, 10_000);
});
