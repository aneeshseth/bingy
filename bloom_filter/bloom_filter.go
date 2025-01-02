package bloomfilter

import (
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	filter []byte
	size   int
}

type userToFilterMap struct {
	m map[string]*BloomFilter
}

type BloomFilterPerUser struct {
	bfMap                         *userToFilterMap
	queueConnForFalsePositives    *kafka.Conn
	queueConnForFilterSizeChanges *kafka.Conn
}

var globalBloomFilter BloomFilterPerUser

func InitializeGlobalBloomFilter() (*BloomFilterPerUser, error) {
	bfMap := &userToFilterMap{
		m: make(map[string]*BloomFilter),
	}

	bf := &BloomFilterPerUser{
		bfMap:                         bfMap,
	}

	globalBloomFilter = *bf

	return bf, nil
}

func NewBloomFilterForUser(size int, userID string) (*BloomFilterPerUser, error) {

	if _, exists := globalBloomFilter.bfMap.m[userID]; exists {
		return nil, nil
	}

	globalBloomFilter.bfMap.m[userID] = &BloomFilter{
		filter: make([]byte, size),
		size:   size,
	}

	return &globalBloomFilter, nil
}

func hashValueAndModBySize(key string, size int) int {
	hasher := murmur3.New32()
	_, _ = hasher.Write([]byte(key))
	hash := hasher.Sum32()
	return int(hash) % size
}

func (bfpu *BloomFilterPerUser) AddToBloomFilterForUser(key string, userID string) error {

	bf, exists := bfpu.bfMap.m[userID]
	if !exists {
		return fmt.Errorf("BloomFilter not found for user: %s", userID)
	}

	idx := hashValueAndModBySize(key, bf.size)

	byteIdx := idx / 8
	bitIdx := idx % 8
	bf.filter[byteIdx] |= 1 << bitIdx

	return nil
}

func (bfpu *BloomFilterPerUser) MembershipCheck(key string, userID string) (bool, error) {
	bf, exists := bfpu.bfMap.m[userID]

	if !exists {
		_, err := NewBloomFilterForUser(1024, userID)
		if err != nil {
			return false, fmt.Errorf("failed to report false positive: %w", err)
		}
		bf, exists = bfpu.bfMap.m[userID]
		if !exists {
			return false, fmt.Errorf("error creating bloom filter for user: %w", err)
		}
	}

	idx := hashValueAndModBySize(key, bf.size)
	byteIdx := idx / 8
	bitIdx := idx % 8
	if bf.filter[byteIdx]&(1<<bitIdx) != 0 {
		return true, nil
	}

	err := bfpu.AddToBloomFilterForUser(key, userID)

	if err != nil {
		return false, nil
	}

	return false, nil
}

// func (bfpu *BloomFilterPerUser) checkFalsePositive(key string) error {
// 	_, err := bfpu.queueConnForFalsePositives.Write([]byte(key))
// 	if err != nil {
// 		return fmt.Errorf("failed to send false positive to Kafka: %w", err)
// 	}
// 	return nil
// }

// func (bfpu *BloomFilterPerUser) changeUserFilterSize(userID string, size int) error {
// 	bf, exists := bfpu.bfMap.m[userID]
// 	if !exists {
// 		return fmt.Errorf("BloomFilter not found for user: %s", userID)
// 	}
// 	bf.size = size
// 	bf.filter = make([]byte, size)
// 	return nil
// }
