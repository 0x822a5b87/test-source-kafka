package kafka.examples;

import kafka.producer.Partitioner;

/**
 * @author 0x822a5b87
 */
public class RoundRobinPartitioner implements Partitioner {

    int i = 0;

    /**
     * 轮询发送数据到partition
     */
    @Override
    public int partition(Object key, int numPartitions) {
        return (++i % numPartitions);
    }
}
