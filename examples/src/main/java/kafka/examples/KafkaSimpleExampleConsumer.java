package kafka.examples;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.LeaderNotAvailableException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka低级消费者示例
 *
 * @author 0x822a5b87
 */
public class KafkaSimpleExampleConsumer {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaSimpleExampleConsumer.class);

    private static final String CLIENT_ID = KafkaSimpleExampleConsumer.class.getName();

    private static final String LEADER_LOOKUP_CLIENT_ID = "leaderLookup";

    private final int soTimeoutMillis;

    private final int bufferSize;

    /**
     * 当前分区所在的broker列表
     */
    private final List<String> replicaBrokers = new ArrayList<>();

    public KafkaSimpleExampleConsumer() {
        this(10 * 1000, 1024 * 64);
    }

    public KafkaSimpleExampleConsumer(int soTimeoutMillis, int bufferSize) {
        this.soTimeoutMillis = soTimeoutMillis;
        this.bufferSize = bufferSize;
    }

    public static void main(String[] args) {
        KafkaSimpleExampleConsumer instance = new KafkaSimpleExampleConsumer();
        instance.run("test", 0, Collections.singletonList("127.0.0.1"), 9092);
    }

    public void run(String topic, int partition, List<String> seedBrokers, int port) {
        //获取partition的元数据
        PartitionMetadata partitionMetadata = findLeader(seedBrokers, port, topic, partition);
        // 如果获取不到partitionMetadata，或者分区的leader不可用则退出
        if (partitionMetadata == null || partitionMetadata.leader() == null) {
            return;
        }
        String leaderBroker = partitionMetadata.leader().host();
        String clientId = "client_" + topic + "_" + partition;
        SimpleConsumer consumer = new SimpleConsumer(leaderBroker, port, soTimeoutMillis, bufferSize, clientId);
        /*
          查询offset，有三种查询方式
          1. 使用OffsetRequest查询
          2. 使用OffsetFetchRequest查询之前提交的offset
          3. 自己在外部存储offset
         */
        long offset = getOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientId);
        int numErrors = 0;
        while (true) {
            //消费失败时重启消费者
            if (consumer == null) {
                consumer = new SimpleConsumer(leaderBroker, port, soTimeoutMillis, bufferSize, clientId);
            }
            //组装FetchRequest消费数据
            FetchRequest fetchRequest = new FetchRequestBuilder().clientId(clientId)
                    .addFetch(topic, partition, offset, 1000)
                    .build();
            FetchResponse response = consumer.fetch(fetchRequest);
            if (response.hasError()) {
                numErrors++;
                short errorCode = response.errorCode(topic, partition);
                if (numErrors > 3) {
                    break;
                }
                //如果offset越界则重新获取offset
                if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
                    offset = getOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientId);
                    continue;
                }
                consumer.close();
                consumer = null;
                leaderBroker = findNewLeader(leaderBroker, topic, partition, port);
                continue;
            }
            numErrors = 0;
            long numRead = 0;
            for (MessageAndOffset messageAndOffset : response.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < offset) {
                    continue;
                }
                /*
                 * 更新offset，有三种方式：
                 * 1. 使用外部存储
                 * 2. 提交到kafka的zk或者特殊topic
                 * 3. 缓存在内存中
                 */
                offset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                numRead++;
                LOG.info("message : [{}]", new String(bytes));
            }
            if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        consumer.close();
    }

    /**
     * 通过发送{@link kafka.javaapi.OffsetRequest}来查询offset
     *
     * @param whileTime while 对应 {@link PartitionOffsetRequestInfo#time()}，取值为 {@link
     *         kafka.api.OffsetRequest#EarliestTime()} 或者 {@link kafka.api.OffsetRequest#LatestTime()} 或者时间戳。
     */
    private long getOffset(SimpleConsumer consumer, String topic, int partitionId, long whileTime, String clientId) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionId);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>(1);
        PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(whileTime, 1);
        requestInfo.put(topicAndPartition, partitionOffsetRequestInfo);
        OffsetRequest offsetRequest = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                                                        clientId);
        OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);
        if (offsetResponse.hasError()) {
            return -1;
        } else {
            long[] offsets = offsetResponse.offsets(topic, partitionId);
            if (offsets.length == 0) {
                LOG.error("offsets.length == [{}]", offsets.length);
                return -1;
            } else if (offsets.length == 1) {
                return offsets[0];
            } else {
                LOG.error("offsets.length == [{}]", offsets.length);
                return -1;
            }
        }
    }

    private PartitionMetadata findLeader(List<String> seedBrokers, int port, String topic, int partitionId) {
        PartitionMetadata metadata = null;
        for (String seedBroker : seedBrokers) {
            SimpleConsumer simpleConsumer = null;
            try {
                //新建一个simpleConsumer用来查询leader
                simpleConsumer = new SimpleConsumer(seedBroker, port, soTimeoutMillis, bufferSize,
                                                    LEADER_LOOKUP_CLIENT_ID);
                //组装TopicMetadataRequest
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(topics);
                TopicMetadataResponse response = simpleConsumer.send(topicMetadataRequest);
                List<TopicMetadata> metadataList = response.topicsMetadata();
                for (TopicMetadata topicMetadata : metadataList) {
                    for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
                        if (partitionMetadata.partitionId() == partitionId) {
                            //注意，这里不需要判断leader，因为我们要找的是PartitionMetadata
                            metadata = partitionMetadata;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("", e);
            } finally {
                if (simpleConsumer != null) {
                    simpleConsumer.close();
                }
            }
            if (metadata != null) {
                replicaBrokers.clear();
                for (Broker replica : metadata.replicas()) {
                    replicaBrokers.add(replica.host());
                }
            }
        }
        return metadata;
    }

    /**
     * 函数用于{@link #findLeader(List, int, String, int)}返回的leader不可用时重新查找leader
     */
    private String findNewLeader(String oldLeader, String topic, int partition, int port) {
        // 重试三次
        for (int i = 0; i < 3; ++i) {
            //此时的seedBrokers为replicaBroker
            PartitionMetadata metadata = findLeader(replicaBrokers, port, topic, partition);
            if (metadata == null || metadata.leader() == null) {
                LOG.warn("metadata and metadata.leader is nul");
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && (i == 0)) {
                LOG.warn("oldLeader equal newLeader : both of them are [{}]", oldLeader);
            } else {
                return metadata.leader().host();
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        throw new LeaderNotAvailableException(String.format("oldLeader equal newLeader : both of them are [%s]", oldLeader));
    }
}
