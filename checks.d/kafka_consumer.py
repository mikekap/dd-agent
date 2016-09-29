# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
from collections import defaultdict

# 3p
from kafka import KafkaClient
from kafka.common import OffsetRequestPayload
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

# project
from checks import AgentCheck

DEFAULT_KAFKA_TIMEOUT = 5
DEFAULT_ZK_TIMEOUT = 5


class KafkaCheck(AgentCheck):

    SOURCE_TYPE_NAME = 'kafka'

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances=instances)
        self.zk_timeout = int(
            init_config.get('zk_timeout', DEFAULT_ZK_TIMEOUT))
        self.kafka_timeout = int(
            init_config.get('kafka_timeout', DEFAULT_KAFKA_TIMEOUT))

    def _get_children(self, zk_conn, zk_path, name_for_error):
        """Fetch child nodes for a given Zookeeper path"""
        children = []
        try:
            children = zk_conn.get_children(zk_path)
        except NoNodeError:
            self.log.warn('No zookeeper node at %s' % zk_path)
        except:
            self.log.exception('Could not read %s from %s' % (name_for_error, zk_path))
        return children

    def check(self, instance):
        # Only validate consumer_groups if specified; fallback to zk otherwise
        consumer_groups_specified = True if 'consumer_groups' in instance else False
        if consumer_groups_specified:
            consumer_groups = self.read_config(instance, 'consumer_groups',
                                               cast=self._validate_consumer_groups)

        zk_connect_str = self.read_config(instance, 'zk_connect_str')
        kafka_host_ports = self.read_config(instance, 'kafka_connect_str')

        # Construct the Zookeeper path pattern
        # /consumers/[groupId]/offsets/[topic]/[partitionId]
        zk_prefix = instance.get('zk_prefix', '')
        zk_path_consumer = zk_prefix + '/consumers/'  # /consumers/
        zk_path_topic_tmpl = zk_path_consumer + '%s/offsets/'  # /consumers/[groupID]/offsets/
        zk_path_partition_tmpl = zk_path_topic_tmpl + '%s/'  # /consumers/[groupID]/offsets/[topic]/

        # Connect to Zookeeper
        zk_conn = KazooClient(zk_connect_str, timeout=self.zk_timeout)
        zk_conn.start()

        # We need to track the topics/partitions for which we're measuring
        # consumer offset so that we can lookup the partition's newest message
        # in the Kafka cluster to calculate consumer lag.
        # This is tricky because different consumer groups may track different
        # partitions of a topic. So when we modify a topic's list of partitions,
        # we don't want to overwrite, only add missing partitions. And only want
        # uniques, because even though each partition may have multiple consumer
        # group offsets, it will only have one broker offset. Hence the
        # defaultdict(set) usage.
        # {topic1: set[(partition1, partition2)], topic2: set[partition1]}
        tracked_topic_partitions = defaultdict(set)

        try:
            # Query Zookeeper for consumer offsets, also fetch consumer_groups,
            # topics, and partitions if not already specified
            consumer_offsets = {}
            # Specifying consumer_groups is optional, if they don't exist, then fetch from ZK
            if not consumer_groups_specified:
                consumer_groups = {consumer_group: None for consumer_group in
                    self._get_children(zk_conn, zk_path_consumer, 'consumer groups')}

            for consumer_group, topics in consumer_groups.iteritems():
                # Specifying topics is optional, if they don't exist, then fetch from ZK
                if topics is None:
                    zk_path_topics = zk_path_topic_tmpl % (consumer_group)
                    topics = {topic: None for topic in
                        self._get_children(zk_conn, zk_path_topics, 'topics')}

                for topic, partitions in topics.iteritems():
                    # Specifying partitions is optional, if they don't exist, then fetch from ZK
                    if partitions is None:
                        zk_path_partitions = zk_path_partition_tmpl % (consumer_group, topic)
                        partitions = self._get_children(zk_conn, zk_path_partitions, 'partitions')

                    # Remember these topic partitions so that we can lookup
                    # the broker offset in the Kafka cluster to calculate
                    # consumer lag. This topic may already have partitions
                    # in tracked_topic_partitions from another consumer group,
                    # so use a set update() to only add missing partitions
                    tracked_topic_partitions[topic].update(set(partitions))

                    # Fetch consumer offsets for each partition from ZK
                    for partition in partitions:
                        zk_path = (zk_path_partition_tmpl + '%s/') % (consumer_group, topic, partition)
                        try:
                            consumer_offset = int(zk_conn.get(zk_path)[0])
                            key = (consumer_group, topic, partition)
                            consumer_offsets[key] = consumer_offset
                        except NoNodeError:
                            self.log.warn('No zookeeper node at %s' % zk_path)
                        except:
                            self.log.exception('Could not read consumer offset from %s' % zk_path)
        finally:
            try:
                zk_conn.stop()
                zk_conn.close()
            except Exception:
                self.log.exception('Error cleaning up Zookeeper connection')

        # Connect to Kafka
        kafka_conn = KafkaClient(kafka_host_ports, timeout=self.kafka_timeout)

        try:
            # Query Kafka to find the latest message committed to the broker
            # so we can calculate consumer lag
            broker_offsets = {}
            for topic, partitions in tracked_topic_partitions.iteritems():
                offset_responses = kafka_conn.send_offset_request([
                    OffsetRequestPayload(topic, p, -1, 1) for p in partitions])

                for resp in offset_responses:
                    key = (resp.topic, resp.partition)
                    broker_offsets[key] = resp.offsets[0]
        finally:
            try:
                kafka_conn.close()
            except Exception:
                self.log.exception('Error cleaning up Kafka connection')

        # Report the broker offset
        # Easiest to do in separate loop from consumer metrics because each
        # topic/partition combo will have one broker offset, but may have
        # multiple consumer offsets
        for (topic, partition), broker_offset in broker_offsets.iteritems():
            broker_tags = ['topic:%s' % topic, 'partition:%s' % partition]
            self.gauge('kafka.broker_offset', broker_offset, tags=broker_tags)

        # Report the consumer offset and lag
        for (consumer_group, topic, partition), consumer_offset in consumer_offsets.iteritems():
            consumer_group_tags = ['topic:%s' % topic, 'partition:%s' % partition,
                'consumer_group:%s' % consumer_group]
            self.gauge('kafka.consumer_offset', consumer_offset, tags=consumer_group_tags)

            broker_offset = broker_offsets.get((topic, partition))
            self.gauge('kafka.consumer_lag', broker_offset - consumer_offset,
               tags=consumer_group_tags)

    # Private config validation/marshalling functions

    def _validate_consumer_groups(self, val):
        # val = {'consumer_group': {'topic': [0, 1]}}
        try:
            # consumer groups are optional
            assert isinstance(val, dict) or val is None
            if isinstance(val, dict):
                for consumer_group, topics in val.iteritems():
                    assert isinstance(consumer_group, (str, unicode))
                    # topics are optional
                    assert isinstance(topics, dict) or topics is None
                    if isinstance(topics, dict):
                        for topic, partitions in topics.iteritems():
                            assert isinstance(topic, (str, unicode))
                            # partitions are optional
                            assert isinstance(partitions, (list, tuple)) or partitions is None
                            if isinstance(partitions, (list, tuple)):
                                for partition in partitions:
                                    assert isinstance(partition, int)
            return val
        except Exception as e:
            self.log.exception(e)
            raise Exception('''The `consumer_groups` value must be a mapping of mappings, like this:
consumer_groups:
  myconsumer0: # consumer group name
    mytopic0: [0, 1] # topic_name: list of partitions
  myconsumer1:
    mytopic0: [0, 1, 2]
    mytopic1: [10, 12]
  myconsumer2:
    mytopic0:
  myconsumer3:

Note that each level of values is optional. Any omitted values will be fetched from Zookeeper.
You can omit partitions (example: myconsumer2), topics (example: myconsumer3), and even consumer_groups.
If a value is omitted, the parent value must still be it's expected type (typically a dict).
''')
