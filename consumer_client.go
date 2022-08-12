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
	"github.com/prometheus/client_golang/prometheus"
)

// ConsumerSingle :
type ConsumerSingle struct {
	Logger           *log.Logger
	Brokers          map[int]string
	ConsumerTopic    string
	Verbose          bool
	Oldest           bool
	PrintMessage     bool
	InitialOffset    int64
	ListTopicHandler map[string]StructFunction
	Connection       sarama.Consumer
	Gauge            *prometheus.Gauge
	EnableAuth       bool
	User             string
	Password         string
	Algorithm        string
	UseTLS           bool
	VerifySSL        bool
}

// NewConsumerSingle :
func NewConsumerSingle(brokers map[int]string, consumerTopic string, verbose, oldest, printMessage bool, gauge *prometheus.Gauge) *ConsumerSingle {
	return &ConsumerSingle{
		Logger:        log.New(os.Stderr, "", log.LstdFlags),
		Brokers:       brokers,
		ConsumerTopic: consumerTopic,
		Verbose:       verbose,
		Oldest:        oldest,
		PrintMessage:  printMessage,
		Gauge:         gauge,
	}
}

// Consume :
func (p *ConsumerSingle) Consume() error {
	if reflect.DeepEqual(p, &ConsumerSingle{}) {
		return fmt.Errorf("ConsumerSingle Configuration is empty, set the needed configuration fron NewConsumerSingle() function")
	}

	p.Logger.Println("| Consumer | Single | Connecting To Kafka | Initializing")

	sortedBrokerNumbers := make([]int, len(p.Brokers))
	i := 0
	for k := range p.Brokers {
		sortedBrokerNumbers[i] = k
		i++
	}
	sort.Ints(sortedBrokerNumbers)

	for _, brokerNumber := range sortedBrokerNumbers {
		brokersList := strings.Split(p.Brokers[brokerNumber], ",")
		topicList := strings.Split(p.ConsumerTopic, ",")
		err := p.CreateConsumerSingleConnection(brokersList, topicList, p.Verbose, p.Oldest)
		if err != nil {
			p.Logger.Printf("| Consumer | Single | Connecting To Kafka | Broker Number : %d | Error | %s\n", brokerNumber, err.Error())
			if p.Gauge != nil {
				(*p.Gauge).Set(2)
			}
			return err
		}

		if p.Gauge != nil {
			(*p.Gauge).Set(1)
		}

		p.Logger.Printf("| Consumer | Single | Connecting To Kafka | Broker Number : %d | Success\n", brokerNumber)

		p.Logger.Println("| Consumer | Single | Broker Number :", brokerNumber, "| Brokers |", brokersList)
		p.Logger.Println("| Consumer | Single | Broker Number :", brokerNumber, "| Topic List |", topicList)

		for _, topic := range topicList {
			done := make(chan bool)
			go func(topic string) {
				partitionList, err := p.Connection.Partitions(topic)
				if err != nil {
					p.Logger.Printf("| Consumer | Single | Broker Number : %d | Error | Topic(%s) | %s\n", brokerNumber, topic, err.Error())
					if p.Gauge != nil {
						(*p.Gauge).Set(2)
					}
					done <- true
					return
				}

				for _, partition := range partitionList {
					if p.Gauge != nil {
						(*p.Gauge).Set(1)
					}

					pc, err := p.Connection.ConsumePartition(topic, partition, p.InitialOffset)
					if err != nil {
						p.Logger.Printf("| Consumer | Single | Broker Number : %d | Error | Topic(%s) | %s\n", brokerNumber, topic, err.Error())
						if p.Gauge != nil {
							(*p.Gauge).Set(2)
						}
						continue
					}

					for {
						select {
						case err := <-pc.Errors():
							p.Logger.Printf("| Consumer | Single | Broker Number : %d | Error | Topic(%s) | %s\n", brokerNumber, topic, err.Error())
							if p.Gauge != nil {
								(*p.Gauge).Set(2)
							}
							continue

						case message := <-pc.Messages():
							if p.PrintMessage == true {
								p.Logger.Printf("| Consumer | Single | Broker Number : %d | %s | %s\n", brokerNumber, topic, string(message.Value))
							}

							p.ExecuteHandlers(message.Topic, message.Value)

						}
					}
				}
				done <- true
			}(topic)
		}

		return nil
	}

	return nil
}

// CreateConsumerSingleConnection :
func (p *ConsumerSingle) CreateConsumerSingleConnection(brokers []string, topic []string, verbose, oldest bool) error {
	if reflect.DeepEqual(p, &ConsumerSingle{}) {
		return fmt.Errorf("ConsumerSingle Configuration is empty, set the needed configuration fron NewConsumerSingle() function")
	}

	var err error

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if len(brokers) <= 0 {
		return fmt.Errorf("Error, Kafka brokers must not empty")
	}

	config := sarama.NewConfig()
	sasl := ScramConfig{
		EnableStatus: p.EnableAuth,
		User:         p.User,
		Password:     p.Password,
		Mechanism:    p.Algorithm,
	}

	sasl.GenerateScramConfig(config)

	config.Net.DialTimeout = time.Duration(1) * time.Second

	p.Connection, err = sarama.NewConsumer(brokers, config)
	if err != nil {
		return err
	}

	if oldest {
		p.InitialOffset = sarama.OffsetOldest
	} else {
		p.InitialOffset = sarama.OffsetNewest
	}

	return nil
}

// Close :
func (p *ConsumerSingle) Close() error {
	if reflect.DeepEqual(p, &ConsumerSingle{}) {
		return fmt.Errorf("ConsumerSingle Configuration is empty, set the needed configuration fron NewConsumerSingle() function")
	}

	err := p.Connection.Close()
	if err != nil {
		return err
	}

	return nil
}

// AddHandler :
func (p *ConsumerSingle) AddHandler(topicName string, handler interface{}, functionName string) {
	if p.ListTopicHandler == nil {
		p.ListTopicHandler = make(map[string]StructFunction)
	}

	p.ListTopicHandler[topicName] = StructFunction{
		HandlerName:  handler,
		FunctionName: functionName,
	}
}

// HandlerNameToString :
func (p *ConsumerSingle) HandlerNameToString(myvar interface{}) (res string) {
	t := reflect.TypeOf(myvar)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
		res += "*"
	}

	return res + t.Name()
}

// ExecuteHandlers :
func (p *ConsumerSingle) ExecuteHandlers(topicName string, topicMessage []byte) {
	usedTopic := p.ListTopicHandler[topicName]

	if usedTopic == (StructFunction{}) {
		p.Logger.Printf("| Consumer | Single | %s | Topic not available in topic handler, topic not handled\n", topicName)
		return
	}

	// for handlerName, functionList := range p.ListTopicHandler {
	meth := reflect.ValueOf(usedTopic.HandlerName).MethodByName(usedTopic.FunctionName)
	if meth.IsValid() == false {
		p.Logger.Printf("| Consumer | Single | %s | Invalid Handler Method : %s.%s\n", topicName, p.HandlerNameToString(usedTopic.HandlerName), usedTopic.FunctionName)
		return
	}

	messageForm := UniformPublishMessage{}
	err := json.Unmarshal(topicMessage, &messageForm)
	if err != nil {
		p.Logger.Printf("| Consumer | Single | %s | Unmarshal Message Error | %s\n", topicName, err.Error())
		return
	}

	if messageForm != (UniformPublishMessage{}) {
		inputs := make([]reflect.Value, 2)
		inputs[0] = reflect.ValueOf(topicName)
		inputs[1] = reflect.ValueOf(messageForm)
		meth.Call(inputs)
	} else {
		p.Logger.Printf("| Consumer | Single | %s | Message is empty\n", topicName)
	}
}
