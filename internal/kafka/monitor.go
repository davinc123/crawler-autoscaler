package kafka

import (
	"github.com/IBM/sarama"
)

type Monitor struct {
	client sarama.Client
	admin  sarama.ClusterAdmin

	topic         string
	consumerGroup string
}

type PartitionLag struct {
	Partition      int32
	LatestOffset   int64
	ConsumerOffset int64
	Lag            int64
}

type LagInfo struct {
	Topic      string
	Partitions []PartitionLag
	TotalLag   int64
}

func NewMonitor(
	brokers []string,
	topic string,
	consumerGroup string,
) (*Monitor, error) {

	config := sarama.NewConfig()

	config.Version = sarama.V4_2_0_0

	client, err := sarama.NewClient(
		brokers,
		config,
	)
	if err != nil {
		return nil, err
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		client.Close()
		return nil, err
	}

	return &Monitor{
		client:        client,
		admin:         admin,
		topic:         topic,
		consumerGroup: consumerGroup,
	}, nil
}

// GetPartitions 获取 Topic 的所有分区
func (m *Monitor) GetPartitions() ([]int32, error) {
	return m.client.Partitions(m.topic)
}

// GetLatestOffset 获取 Latest Offset
func (m *Monitor) GetLatestOffset(partition int32) (int64, error) {
	return m.client.GetOffset(
		m.topic,
		partition,
		sarama.OffsetNewest,
	)
}

// GetConsumerOffsets 获取 Consumer Group 当前 Offset
func (m *Monitor) GetConsumerOffsets() (
	map[int32]int64,
	error,
) {
	partitions, err := m.client.Partitions(m.topic)
	if err != nil {
		return nil, err
	}

	result, err := m.admin.ListConsumerGroupOffsets(
		m.consumerGroup,
		map[string][]int32{
			m.topic: partitions,
		},
	)
	if err != nil {
		return nil, err
	}

	offsets := make(map[int32]int64)

	block, ok := result.Blocks[m.topic]
	if !ok {
		return offsets, nil
	}

	for partition, offset := range block {
		offsets[partition] = offset.Offset
	}

	return offsets, nil
}

// GetLag 计算 Lag
func (m *Monitor) GetLag() (*LagInfo, error) {

	partitions, err := m.client.Partitions(m.topic)
	if err != nil {
		return nil, err
	}

	consumerOffsets, err := m.GetConsumerOffsets()
	if err != nil {
		return nil, err
	}

	result := &LagInfo{
		Topic: m.topic,
	}

	for _, partition := range partitions {

		latestOffset, err := m.client.GetOffset(
			m.topic,
			partition,
			sarama.OffsetNewest,
		)
		if err != nil {
			return nil, err
		}

		consumerOffset, ok := consumerOffsets[partition]

		if !ok || consumerOffset < 0 {
			consumerOffset = 0
		}

		lag := latestOffset - consumerOffset

		if lag < 0 {
			lag = 0
		}

		result.Partitions = append(
			result.Partitions,
			PartitionLag{
				Partition:      partition,
				LatestOffset:   latestOffset,
				ConsumerOffset: consumerOffset,
				Lag:            lag,
			},
		)

		result.TotalLag += lag
	}

	return result, nil
}
