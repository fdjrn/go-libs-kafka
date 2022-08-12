package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

// ConsumerGroup :
type ConsumerGroup struct {
	Logger           *log.Logger
	Brokers          map[int]string
	ConsumerTopic    string
	ConsumerGroup    string
	Assignor         string
	Version          string
	Verbose          bool
	Oldest           bool
	PrintMessage     bool
	ListTopicHandler map[string]StructFunction
	Connection       sarama.ConsumerGroup
	Gauge            *prometheus.Gauge
	EnableAuth       bool
	User             string
	Password         string
	Algorithm        string
	UseTLS           bool
	VerifySSL        bool
}

// NewConsumerGroup :
func NewConsumerGroup(
	brokers map[int]string,
	consumerTopic,
	consumerGroup,
	assignor,
	version string,
	verbose,
	oldest,
	printMessage bool,
	gauge *prometheus.Gauge,
	enableAuth bool,
	user string,
	password string,
	algorithm string,
	useTLS bool,
	verifySSL bool) *ConsumerGroup {
	return &ConsumerGroup{
		Logger:        log.New(os.Stderr, "", log.LstdFlags),
		Brokers:       brokers,
		ConsumerTopic: consumerTopic,
		ConsumerGroup: consumerGroup,
		Assignor:      assignor,
		Version:       version,
		Verbose:       verbose,
		Oldest:        oldest,
		PrintMessage:  printMessage,
		Gauge:         gauge,
		EnableAuth:    enableAuth,
		User:          user,
		Password:      password,
		Algorithm:     algorithm,
		UseTLS:        useTLS,
		VerifySSL:     verifySSL,
	}
}

// Consume :
func (p *ConsumerGroup) Consume() error {
	if reflect.DeepEqual(p, &ConsumerGroup{}) {
		return fmt.Errorf("ConsumerGroup Configuration is empty, set the needed configuration fron NewConsumerGroup() function")
	}

	p.Logger.Println("| Consumer | Group | Connecting To Kafka | Initializing")

	sortedBrokerNumbers := make([]int, len(p.Brokers))
	i := 0
	for k := range p.Brokers {
		sortedBrokerNumbers[i] = k
		i++
	}
	sort.Ints(sortedBrokerNumbers)

	for _, brokerNumber := range sortedBrokerNumbers {
		p.Logger.Printf("| Consumer | Group | Connecting To Kafka | Broker Number : %d\n", brokerNumber)

		brokersList := strings.Split(p.Brokers[brokerNumber], ",")
		topics := strings.Split(p.ConsumerTopic, ",")
		err := p.CreateConsumerGroupConnection(
			brokersList,
			topics,
			p.ConsumerGroup,
			p.Assignor,
			p.Version,
			p.Verbose,
			p.Oldest,
		)
		if err != nil {
			p.Logger.Printf("| Consumer | Group | Connecting To Kafka | Broker Number : %d | Error | %s\n", brokerNumber, err.Error())
			continue
		}

		p.Logger.Printf("| Consumer | Group | Connecting To Kafka | Broker Number : %d | Success\n", brokerNumber)
		/**
		 * Setup a new Sarama consumer group
		 */
		consumer := Consumer{
			ready:               make(chan bool),
			consumerGroupConfig: p,
		}

		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := p.Connection.Consume(ctx, topics, &consumer); err != nil {
					p.Logger.Printf("| Consumer | Group | Broker Number : %d | Error | From Consumer : %v\n", brokerNumber, err)
					if p.Gauge != nil {
						(*p.Gauge).Set(2)
					}
				} else {
					if p.Gauge != nil {
						(*p.Gauge).Set(1)
					}
				}
				// check if context was cancelled, signaling that the consumer should stop
				if ctx.Err() != nil {
					p.Logger.Printf("| Consumer | Group | Broker Number : %d | Error | Context Error : %s\n", brokerNumber, ctx.Err())
					return
				}
				consumer.ready = make(chan bool)
			}
		}()

		<-consumer.ready // Await till the consumer has been set up
		p.Logger.Println("| Consumer | Group | Broker Number :", brokerNumber, "| Brokers |", brokersList)
		p.Logger.Println("| Consumer | Group | Broker Number :", brokerNumber, "| Topic List |", topics)

		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-ctx.Done():
			p.Logger.Printf("| Consumer | Group | Broker Number : %d | Terminating & Closing | Context cancelled", brokerNumber)

			if p.Gauge != nil {
				(*p.Gauge).Set(2)
			}
		case <-sigterm:
			p.Logger.Printf("| Consumer | Group | Broker Number : %d | Terminating & Closing | Via signal", brokerNumber)
			if p.Gauge != nil {
				(*p.Gauge).Set(2)
			}
		}
		cancel()
		wg.Wait()

		p.Logger.Printf("| Consumer | Group | Broker Number : %d | Closing Connection", brokerNumber)
		if err := p.Connection.Close(); err != nil {
			p.Logger.Printf("| Consumer | Group | Broker Number : %d | Closing Connection | Error | %v", brokerNumber, err)
			if p.Gauge != nil {
				(*p.Gauge).Set(2)
			}
			continue
		}

		return nil
	}

	if p.Gauge != nil {
		(*p.Gauge).Set(2)
	}

	return fmt.Errorf("unable to connect to any configured brokers")
}

// CreateConsumerGroupConnection :
func (p *ConsumerGroup) CreateConsumerGroupConnection(brokers []string, topics []string, group, assignor, version string, verbose, oldest bool) error {
	if reflect.DeepEqual(p, &ConsumerGroup{}) {
		return fmt.Errorf("ConsumerGroup Configuration is empty, set the needed configuration fron NewConsumerGroup() function")
	}

	if len(brokers) == 0 {
		return fmt.Errorf("error, brokers must not empty")
	}

	if len(topics) == 0 {
		return fmt.Errorf("error, topics must not empty")
	}

	if group == "" {
		return fmt.Errorf("error, group must not empty")
	}

	if assignor == "" {
		return fmt.Errorf("error, assignor must not empty")
	}

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if version == "" {
		version = "2.1.1"
	}

	kafkaVersion, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return fmt.Errorf("error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = kafkaVersion

	sasl := ScramConfig{
		EnableStatus: p.EnableAuth,
		User:         p.User,
		Password:     p.Password,
		Mechanism:    p.Algorithm,
		UseTLS:       p.UseTLS,
		VerifySSL:    p.VerifySSL,
	}

	sasl.GenerateScramConfig(config)

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		return fmt.Errorf("unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	p.Connection, err = sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		return err
	}

	return nil
}

// Close :
func (p *ConsumerGroup) Close() error {
	if reflect.DeepEqual(p, &ConsumerGroup{}) {
		return fmt.Errorf("ConsumerGroup Configuration is empty, set the needed configuration fron NewConsumerGroup() function")
	}

	err := p.Connection.Close()
	if err != nil {
		return err
	}

	return nil
}

// AddHandler :
func (p *ConsumerGroup) AddHandler(topicName string, handler interface{}, functionName string) {
	if p.ListTopicHandler == nil {
		p.ListTopicHandler = make(map[string]StructFunction)
	}

	p.ListTopicHandler[topicName] = StructFunction{
		HandlerName:  handler,
		FunctionName: functionName,
	}
}

// HandlerNameToString :
func (p *ConsumerGroup) HandlerNameToString(myvar interface{}) (res string) {
	t := reflect.TypeOf(myvar)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
		res += "*"
	}

	return res + t.Name()
}

// ExecuteHandlers :
func (p *ConsumerGroup) ExecuteHandlers(topicName string, topicMessage []byte) {
	usedTopic := p.ListTopicHandler[topicName]

	if usedTopic == (StructFunction{}) {
		p.Logger.Printf("| Consumer | Group | %s | Topic not available in topic handler, topic not handled\n", topicName)
		return
	}

	// for handlerName, functionList := range p.ListTopicHandler {
	meth := reflect.ValueOf(usedTopic.HandlerName).MethodByName(usedTopic.FunctionName)
	if meth.IsValid() == false {
		p.Logger.Printf("| Consumer | Group | %s | Invalid Handler Method : %s.%s\n", topicName, p.HandlerNameToString(usedTopic.HandlerName), usedTopic.FunctionName)
		return
	}

	messageForm := UniformPublishMessage{}
	err := json.Unmarshal(topicMessage, &messageForm)
	if err != nil {
		p.Logger.Printf("| Consumer | Group | %s | Unmarshal Message Error | %s\n", topicName, err.Error())
		return
	}

	if messageForm != (UniformPublishMessage{}) {
		inputs := make([]reflect.Value, 2)
		inputs[0] = reflect.ValueOf(topicName)
		inputs[1] = reflect.ValueOf(messageForm)
		meth.Call(inputs)
	} else {
		p.Logger.Printf("| Consumer | Group | %s | Message is empty\n", topicName)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	consumerGroupConfig *ConsumerGroup
	ready               chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	if consumer.consumerGroupConfig.Gauge != nil {
		(*consumer.consumerGroupConfig.Gauge).Set(1)
	}
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if consumer.consumerGroupConfig.PrintMessage == true {
			consumer.consumerGroupConfig.Logger.Printf("| Consumer | Group | %s | %s\n", message.Topic, string(message.Value))
		}

		session.MarkMessage(message, "")

		consumer.consumerGroupConfig.ExecuteHandlers(message.Topic, message.Value)
	}

	return nil
}
