package models

import (
	"errors"
	"fmt"
	"io"
	"encoding/binary"
	"bufio"
)

type ClusterMetadataLogFile struct {
	RecordButch []RecordButch
}


func (f *ClusterMetadataLogFile) InitFromFile(file *bufio.Reader) error{
	for {
		var recordButch RecordButch
		err := binary.Read(file, binary.BigEndian, &recordButch.BaseOffset)
		if err != nil {
			// Если дошли до конца файла — аккуратно выходим из цикла
			if errors.Is(err, io.EOF) {
				break 
			}

			// Любая другая критическая ошибка (ошибка диска и т.д.)
			return fmt.Errorf("Ошибка при чтении файла: %v", err)
		}

		err = recordButch.InitFromFile(file)
		if err != nil {
			return fmt.Errorf("Ошибка при чтении файла: %v", err)
		}

		f.RecordButch = append(f.RecordButch, recordButch)
	}
	return nil
}

type RecordButch struct {
	// BaseOffset is a 8-byte big-endian integer indicating the offset of the first record in this batch
	BaseOffset [8]byte
	// BatchLength is a 4-byte big-endian integer indicating the length of the entire record batch in bytes
	BatchLength [4]byte
	// PartitionLeaderEpoch is a 4-byte big-endian integer indicating the epoch of the leader for this partition. It is a monotonically increasing number that is incremented by 1 whenever the partition leader changes. This value is used to detect out of order writes.
	PartitionLeaderEpoch [4]byte
	// MagicByte is a 1-byte integer indicating the version of the record batch format. This value is used to evolve the record batch format in a backward-compatible way.
	MagicByte byte
	// CRC is a 4-byte big-endian integer indicating the CRC32-C checksum of the record batch.
	CRC [4]byte
	// Attributes is a 2-byte big-endian integer indicating the attributes of the record batch.
	Attributes [2]byte
	// LastOffsetDelta is a 4-byte big-endian integer indicating the difference between the last offset of this record batch and the base offset.
	LastOffsetDelta [4]byte
	// Base Timestamp is a 8-byte big-endian integer indicating the timestamp of the first record in this batch.
	BaseTimestamp [8]byte
	// MaxTimestamp is a 8-byte big-endian integer indicating the maximum timestamp of the records in this batch.
	MaxTimestamp [8]byte
	// ProducerID is a 8-byte big-endian integer indicating the ID of the producer that produced the records in this batch.
	ProducerID [8]byte
	// ProducerEpoch is a 2-byte big-endian integer indicating the epoch of the producer that produced the records in this batch.
	ProducerEpoch [2]byte
	// BaseSequence is a 4-byte big-endian integer indicating the sequence number of the first record in a batch. It is used to ensure the correct ordering and deduplication of messages produced by a Kafka producer.
	BaseSequence [4]byte
	// RecordsLength is a 4-byte big-endian integer indicating the number of records in this batch.
	RecordsLength uint32
	Records []Record
}

func (r *RecordButch) InitFromFile(file *bufio.Reader) error{
	err := binary.Read(file, binary.BigEndian, &r.BatchLength)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.PartitionLeaderEpoch)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.MagicByte)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.CRC)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.Attributes)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.LastOffsetDelta)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.BaseTimestamp)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.MaxTimestamp)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.ProducerID)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.ProducerEpoch)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.BaseSequence)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.RecordsLength)
	if err != nil {
		return err
	}

	for range r.RecordsLength {
		var record Record
		err:= record.InitFromFile(file)
		if err != nil {
			return err
		}

		r.Records = append(r.Records, record)
	}

	return nil
}

type Record struct {
	// Length is a signed variable size integer indicating the length of the record, the length is calculated from the attributes field to the end of the record.
	Length int64
	// Attributes is a 1-byte integer indicating the attributes of the record. Currently, this field is unused in the protocol.
	Attributes byte
	// TimestampDelta is a signed variable size integer indicating the difference between the timestamp of the record and the base timestamp of the record batch.
	TimestampDelta int64
	// OffsetDelta is a signed variable size integer indicating the difference between the offset of the record and the base offset of the record batch.
	OffsetDelta int64
	// KeyLength is a signed variable size integer indicating the length of the key of the record.
	KeyLength int64
	// Key is a byte array indicating the key of the record.
	Key []byte
	// ValueLength is a signed variable size integer indicating the length of the value of the record.
	ValueLength int64
	// Value is a byte array indicating the value of the record.
	Value any
	// HeadersArrayCount is an signed variable size integer indicating the number of headers present.
	HeadersArrayCount int64
}

func (r *Record) InitFromFile(file *bufio.Reader) error{
	var err error

	r.Length, err = binary.ReadVarint(file)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.Attributes)
	if err != nil {
		return err
	}

	r.TimestampDelta, err = binary.ReadVarint(file)
	if err != nil {
		return err
	}

	r.OffsetDelta, err = binary.ReadVarint(file)
	if err != nil {
		return err
	}

	r.KeyLength, err = binary.ReadVarint(file)
	if err != nil {
		return err
	}

	r.Key = nil


	r.ValueLength, err = binary.ReadVarint(file)
	if err != nil {
		return err
	}

	var valueFrameVersion byte
	err = binary.Read(file, binary.BigEndian, &valueFrameVersion)
	if err != nil {
		return err
	}

	var valueType uint8
	err = binary.Read(file, binary.BigEndian, &valueType)
	if err != nil {
		return err
	}

	newObject,ok := factoryRegistry[valueType]
	if !ok {
		return fmt.Errorf("Тип не найден")
	}


	value := newObject()
	err = value.InitFromFile(file, valueFrameVersion, valueType)
	if err != nil {
		return err
	}

	r.Value = value

	r.HeadersArrayCount, err = binary.ReadVarint(file)
	if err != nil {
		return err
	}

	return nil
}

type Initer interface {
	InitFromFile(file *bufio.Reader, frameVersion byte, recordType uint8) error
}


var factoryRegistry = map[uint8]func() Initer{
	12: func() Initer { return &FeatureLevelRecord{} },
	2: func() Initer { return &TopicRecord{} },
	3: func() Initer { return &PartitionRecord{} },
}


type FeatureLevelRecord struct {
	// FrameVersion is a 1-byte integer indicating the version of the format of the record.
	FrameVersion byte
	// Type is a 1-byte integer indicating the type of the record.
	Type uint8
	// Version is a 1-byte integer indicating the version of the feature level record.
	Version byte
	// NameLength is a unsigned variable size integer indicating the length of the name. But, as name is a compact string, the length of the name is always length - 1.
	NameLength uint64
	// Name is a byte array parsed as a string indicating the name of the feature level record.
	Name string
	// FeatureLevel is a 2-byte big-endian integer indicating the level of the feature.
	FeatureLevel uint16
	// TaggedFieldCount is an unsigned variable size integer indicating the number of tagged fields.
	TaggedFieldCount uint64
}

func (r *FeatureLevelRecord) InitFromFile(file *bufio.Reader, frameVersion byte, recordType uint8) error{
	r.FrameVersion = frameVersion
	r.Type = recordType

	err := binary.Read(file, binary.BigEndian, &r.Version)
	if err != nil {
		return err
	}

	r.NameLength, err = binary.ReadUvarint(file)
	if err != nil {
		return err
	}

	bufStr := make([]byte, r.NameLength-1)
	err = binary.Read(file, binary.BigEndian, &bufStr)
	if err != nil {
		return err
	}
	r.Name = string(bufStr)

	err = binary.Read(file, binary.BigEndian, &r.FeatureLevel)
	if err != nil {
		return err
	}

	r.TaggedFieldCount, err = binary.ReadUvarint(file)
	if err != nil {
		return err
	}

	return nil
}

type TopicRecord struct {
	// FrameVersion is a 1-byte integer indicating the version of the format of the record.
	FrameVersion byte
	// Type is a 1-byte integer indicating the type of the record.
	Type byte
	// Version is a 1-byte integer indicating the version of the topic record.
	Version byte
	// NameLength is a unsigned variable size integer indicating the length of the name. But, as name is a compact string, the length of the name is always length - 1.
	NameLength uint64
	// TopicName is a byte array parsed as a string indicating the name of the topic.
	TopicName string
	// TopicUUID is a 16-byte raw byte array indicating the UUID of the topic.
	TopicUUID [16]byte
	// TaggedFieldCount is an unsigned variable size integer indicating the number of tagged fields.
	TaggedFieldCount uint64
}

func (r *TopicRecord) InitFromFile(file *bufio.Reader, frameVersion byte, recordType uint8) error{
	r.FrameVersion = frameVersion
	r.Type = recordType

	err := binary.Read(file, binary.BigEndian, &r.Version)
	if err != nil {
		return err
	}

	r.NameLength, err = binary.ReadUvarint(file)
	if err != nil {
		return err
	}

	bufStr := make([]byte, r.NameLength-1)
	err = binary.Read(file, binary.BigEndian, &bufStr)
	if err != nil {
		return err
	}
	r.TopicName = string(bufStr)

	err = binary.Read(file, binary.BigEndian, &r.TopicUUID)
	if err != nil {
		return err
	}

	r.TaggedFieldCount, err = binary.ReadUvarint(file)
	if err != nil {
		return err
	}

	return nil
}

type PartitionRecord struct {
	// FrameVersion is a 1-byte integer indicating the version of the format of the record.
	FrameVersion byte
	// Type is a 1-byte integer indicating the type of the record.
	Type byte
	// Version is a 1-byte integer indicating the version of the partition record.
	Version byte
	// PartitionID is a 4-byte big-endian integer indicating the ID of the partition.
	PartitionID uint32
	// TopicUUID is a 16-byte raw byte array indicating the UUID of the topic.
	TopicUUID [16]byte
	// LengthOfReplicaArray is an unsigned variable size integer indicating the number of replicas in the replica array.
	LengthOfReplicaArray uint64
	// ReplicaArray is a compact array of 4-byte big-endian integers, containing the replica ID of the replicas.
	ReplicaArray uint32
	// LengthOfInSyncReplicaArray is an unsigned variable size integer indicating the number of replicas in the In Sync Replica array.
	LengthOfInSyncReplicaArray uint64
	// InSyncReplicaArray is a compact array of 4-byte big-endian integers, containing the replica ID of the in sync replicas.
	InSyncReplicaArray uint32
	// LengthOfRemovingReplicasArray is an unsigned variable size integer indicating the number of replicas in the Removing Replicas array.
	LengthOfRemovingReplicasArray uint64
	// LengthOfAddingReplicasArray is an unsigned variable size integer indicating the number of replicas in the Adding Replicas array.
	LengthOfAddingReplicasArray uint64
	// Leader is a 4-byte big-endian integer indicating the replica ID of the leader.
	Leader uint32
	// LeaderEpoch is a 4-byte big-endian integer indicating the epoch of the leader.
	LeaderEpoch uint32
	// PartitionEpoch is a 4-byte big-endian integer indicating the epoch of the partition.
	PartitionEpoch uint32
	// LengthOfDirectoriesArray is an unsigned variable size integer indicating the number of directories in the Directories array.
	LengthOfDirectoriesArray uint64
	// DirectoriesArray is a compact array of 16-byte raw byte arrays, containing the UUID of the directories.
	DirectoriesArray [16]byte
	// TaggedFieldCount is an unsigned variable size integer indicating the number of tagged fields.
	TaggedFieldCount uint64
}

func (r *PartitionRecord) InitFromFile(file *bufio.Reader, frameVersion byte, recordType uint8) error{
	r.FrameVersion = frameVersion
	r.Type = recordType

	err := binary.Read(file, binary.BigEndian, &r.Version)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.PartitionID)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.TopicUUID)
	if err != nil {
		return err
	}

	r.LengthOfReplicaArray, err = binary.ReadUvarint(file)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.ReplicaArray)
	if err != nil {
		return err
	}

	r.LengthOfInSyncReplicaArray, err = binary.ReadUvarint(file)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.InSyncReplicaArray)
	if err != nil {
		return err
	}

	r.LengthOfRemovingReplicasArray, err = binary.ReadUvarint(file)
	if err != nil {
		return err
	}

	r.LengthOfAddingReplicasArray, err = binary.ReadUvarint(file)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.Leader)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.LeaderEpoch)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.PartitionEpoch)
	if err != nil {
		return err
	}

	r.LengthOfDirectoriesArray, err = binary.ReadUvarint(file)
	if err != nil {
		return err
	}

	err = binary.Read(file, binary.BigEndian, &r.DirectoriesArray)
	if err != nil {
		return err
	}

	r.TaggedFieldCount, err = binary.ReadUvarint(file)
	if err != nil {
		return err
	}

	return nil
}
