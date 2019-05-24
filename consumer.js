
/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

//var Transform = require('stream').Transform;

var Kafka = require('node-rdkafka');

const defaultConfig = {
  // The group.id and metadata.broker.list properties are required for a consumer
  'group.id': 'my-group1',
  'metadata.broker.list': 'localhost:9092'
}

const topicConf = {
  /*
    See the reason
  
    https://github.com/Blizzard/node-rdkafka/issues/437#issuecomment-406129883
    https://github.com/Blizzard/node-rdkafka/issues/495
    https://cwiki.apache.org/confluence/display/KAFKA/FAQ#FAQ-Whydoesmyconsumernevergetanydata?
 */
  'auto.offset.reset': 'earliest' // <-- THIS OPTIONS
}

const consumer = new Kafka.KafkaConsumer(defaultConfig, topicConf)

consumer.on('ready', () => {
  console.log('Consumer was connected successfully!')

  consumer.subscribe(['test'])
  /*
    Standard API
    https://github.com/Blizzard/node-rdkafka#standard-api-1
  */
  consumer.consume();
})

consumer.on('data', (message) => {
  console.log('on:data. Message found: ', message)
  // consumer.commitMessageSync(message)
  consumer.disconnect()
})

consumer.on('event.error', (err) => {
  console.error('Error from consumer')
  console.error(err)
})

consumer.connect()

//stopping this example after 30s
setTimeout(function() {
  console.log('Timeout is exceeded. Disconnecting...')
  consumer.disconnect()
}, 70000)