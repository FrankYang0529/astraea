/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.homework;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class YourPartitioner implements Partitioner {

  private Map<String, Map<Integer, Long>> topicPartitionToCount;
  private Map<Integer, Long> brokerToCount;

  // get your magic configs
  @Override
  public void configure(Map<String, ?> configs) {
    topicPartitionToCount = new HashMap<>();
    brokerToCount = new HashMap<>();
  }

  // write your magic code
  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    var partitions = cluster.availablePartitionsForTopic(topic);
    // no available partition so we return -1
    if (partitions.isEmpty()) return -1;

    for (var node : cluster.nodes()) {
      brokerToCount.computeIfAbsent(node.id(), id -> 0L);
    }

    topicPartitionToCount.computeIfAbsent(topic, t -> new HashMap<>());
    for (var partition : partitions) {
      topicPartitionToCount.get(topic).computeIfAbsent(partition.partition(), p -> 0L);
    }

    var brokerToPartitions = new HashMap<Integer, Set<Integer>>();
    for (var partition : partitions) {
      var broker = partition.leader().id();
      brokerToPartitions.computeIfAbsent(broker, id -> new HashSet<Integer>());
      brokerToPartitions.get(broker).add(partition.partition());
    }

    // sort broker by count
    var brokerIterator =
        brokerToCount.entrySet().stream()
            .sorted(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .iterator();
    while (brokerIterator.hasNext()) {
      var broker = brokerIterator.next();
      var partitionSet = brokerToPartitions.get(broker);
      if (partitionSet == null) continue;

      // sort partition by count
      var partition =
          topicPartitionToCount.get(topic).entrySet().stream()
              .filter(entry -> partitionSet.contains(entry.getKey()))
              .sorted(Map.Entry.comparingByValue())
              .map(Map.Entry::getKey)
              .findFirst();
      if (partition.isPresent()) {
        brokerToCount.put(broker, brokerToCount.get(broker) + 1);
        topicPartitionToCount
            .get(topic)
            .put(partition.get(), topicPartitionToCount.get(topic).get(partition.get()) + 1);
        return partition.get();
      }
    }

    return -1;
  }

  @Override
  public void close() {}
}
