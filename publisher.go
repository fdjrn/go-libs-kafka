package kafka

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

// Publisher :
type Publisher struct {
	Logger             *log.Logger
	Brokers            map[int]string
	Verbose            bool
	DialTimeout        int
	ProducerRetryMax   int
	ProducerTimeout    int
	ProducerIdempotent bool
	Version            string
	Producer           sarama.SyncProducer
	EnableAuth         bool
	User               string
	Password           string
	Algorithm          string
}

// NewKafkaPublisher :
func NewKafkaPublisher(brokers map[int]string, verbose bool, dialTimeout int, retryMax int, timeout int, idempotent bool, version string) *Publisher {
	return &Publisher{
		Logger:             log.New(os.Stderr, "", log.LstdFlags),
		Brokers:            brokers,
		Verbose:            verbose,
		DialTimeout:        dialTimeout,
		ProducerRetryMax:   retryMax,
		ProducerTimeout:    timeout,
		ProducerIdempotent: idempotent,
		Version:            version,
	}
}

// CreatePublisherConnection :
func (p *Publisher) CreatePublisherConnection() error {
	if reflect.DeepEqual(p, &Publisher{}) {
		return fmt.Errorf("Publisher Configuration is empty, set the needed configuration fron NewKafkaPublisher() function")
	}

	p.Logger.Println("| Publisher | Connecting To Kafka | Initializing")

	sortedBrokerNumbers := make([]int, len(p.Brokers))
	i := 0
	for k := range p.Brokers {
		sortedBrokerNumbers[i] = k
		i++
	}
	sort.Ints(sortedBrokerNumbers)

	for _, brokerNumber := range sortedBrokerNumbers {
		p.Logger.Printf("| Publisher | Connecting To Kafka | Broker Number : %d\n", brokerNumber)
		brokers := strings.Split(p.Brokers[brokerNumber], ",")
		err := p.Connecting(
			brokers,
			p.Verbose,
			p.DialTimeout,
			p.ProducerRetryMax,
			p.ProducerTimeout,
			p.ProducerIdempotent,
			p.Version,
		)
		if err != nil {
			p.Logger.Printf("| Publisher | Connecting To Kafka | Broker Number : %d | Error | %s\n", brokerNumber, err.Error())
			continue
		}

		p.Logger.Printf("| Publisher | Connecting To Kafka | Broker Number : %d | Success\n", brokerNumber)

		return nil
	}

	return fmt.Errorf("unable to connect to any configured brokers")
}

// Connecting :
func (p *Publisher) Connecting(brokers []string, verbose bool, dialTimeout int, producerRetryMax int, producerTimeout int, idempotent bool, version string) error {
	if reflect.DeepEqual(p, &Publisher{}) {
		return fmt.Errorf("Publisher Configuration is empty, set the needed configuration fron NewKafkaPublisher() function")
	}

	config := sarama.NewConfig()

	sasl := ScramConfig{
		EnableStatus: p.EnableAuth,
		User:         p.User,
		Password:     p.Password,
		Mechanism:    p.Algorithm,
	}

	sasl.GenerateScramConfig(config)

	if dialTimeout > 0 {
		config.Net.DialTimeout = time.Duration(dialTimeout) * time.Second
	}
	if producerRetryMax > 0 {
		config.Producer.Retry.Max = producerRetryMax
	}
	if producerTimeout > 0 {
		config.Producer.Timeout = time.Duration(producerTimeout) * time.Second
	}
	if version == "" {
		version = "2.1.1"
	}

	kafkaVersion, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return fmt.Errorf("error parsing Kafka version: %v", err)
	}

	config.Version = kafkaVersion
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Return.Successes = true

	if idempotent == true {
		config.Producer.Idempotent = idempotent
		config.Net.MaxOpenRequests = 1
	}

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if len(brokers) <= 0 {
		return fmt.Errorf("error, Kafka brokers must not empty")
	}

	p.Producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return err
	}

	return nil
}

// Publish :
func (p *Publisher) Publish(jsonData interface{}, topic interface{}) error {
	if reflect.DeepEqual(p, &Publisher{}) {
		return fmt.Errorf("Publisher Configuration is empty, set the needed configuration fron NewKafkaPublisher() function")
	}

	sendData, err := json.Marshal(jsonData)
	if err != nil {
		return err
	}

	if topic == nil {
		return fmt.Errorf("topic cannot be empty")
	}

	partition, offset, err := p.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic.(string),
		Value: sarama.StringEncoder(sendData),
	})
	if err != nil {
		return err
	}

	p.Logger.Printf("| Publisher | Message is Stored | Topic(%s) | Partition(%d) | Offset(%d)\n", topic.(string), partition, offset)

	return nil
}

// SilentPublish :
func (p *Publisher) SilentPublish(jsonData interface{}, topic interface{}) (int32, int64, error) {
	if reflect.DeepEqual(p, &Publisher{}) {
		return 0, 0, fmt.Errorf("Publisher Configuration is empty, set the needed configuration fron NewKafkaPublisher() function")
	}

	sendData, err := json.Marshal(jsonData)
	if err != nil {
		return 0, 0, err
	}

	if topic == nil {
		return 0, 0, fmt.Errorf("topic cannot be empty")
	}

	partition, offset, err := p.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic.(string),
		Value: sarama.StringEncoder(sendData),
	})
	if err != nil {
		return 0, 0, err
	}

	return partition, offset, nil
}

// UniformPublishMessage :
type UniformPublishMessage struct {
	EventName string      `json:"event_name"`
	Type      string      `json:"type"`
	Status    string      `json:"status"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data"`
}

// UniformPublish :
func (p *Publisher) UniformPublish(eventName, types, status, message string, data interface{}, topic interface{}) error {
	if reflect.DeepEqual(p, &Publisher{}) {
		return fmt.Errorf("Publisher Configuration is empty, set the needed configuration fron NewKafkaPublisher() function")
	}
	publishedMessage := UniformPublishMessage{
		EventName: eventName,
		Type:      types,
		Status:    status,
		Message:   message,
		Data:      data,
	}

	if topic == nil {
		return fmt.Errorf("topic cannot be empty")
	}

	err := p.Publish(&publishedMessage, topic.(string))
	if err != nil {
		return err
	}

	return nil
}

// SilentUniformPublish :
func (p *Publisher) SilentUniformPublish(eventName, types, status, message string, data interface{}, topic interface{}) error {
	if reflect.DeepEqual(p, &Publisher{}) {
		return fmt.Errorf("Publisher Configuration is empty, set the needed configuration fron NewKafkaPublisher() function")
	}
	publishedMessage := UniformPublishMessage{
		EventName: eventName,
		Type:      types,
		Status:    status,
		Message:   message,
		Data:      data,
	}

	if topic == nil {
		return fmt.Errorf("topic cannot be empty")
	}

	_, _, err := p.SilentPublish(&publishedMessage, topic.(string))
	if err != nil {
		return err
	}

	return nil
}

// Close :
func (p *Publisher) Close() error {
	err := p.Producer.Close()
	if err != nil {
		return err
	}

	return nil
}
