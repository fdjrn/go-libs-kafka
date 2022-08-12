package kafka

// ConfKafka :
type ConfKafka struct {
	Brokers            map[int]string `mapstructure:"brokers"`
	PublisherTopic     string         `mapstructure:"publisherTopic"`
	ConsumerTopic      string         `mapstructure:"consumerTopic"`
	ConsumerGroup      string         `mapstructure:"consumerGroup"`
	Assignor           string         `mapstructure:"assignor"`
	Version            string         `mapstructure:"version"`
	Verbose            bool           `mapstructure:"verbose"`
	DialTimeout        int            `mapstructure:"dialTimeout"`
	ProducerRetryMax   int            `mapstructure:"producerRetryMax"`
	ProducerTimeout    int            `mapstructure:"producerTimeout"`
	ProducerIdempotent bool           `mapstructure:"producerIdempotent"`
	Oldest             bool           `mapstructure:"oldest"`
	ConsumerType       string         `mapstructure:"consumerType"`
	ConsumerStatus     bool           `mapstructure:"consumerStatus"`
	PublisherStatus    bool           `mapstructure:"publisherStatus"`
	EnableAuth         bool           `mapstructure:"enableAuth"`
	User               string         `mapstructure:"user"`
	Password           string         `mapstructure:"password"`
	Algorithm          string         `mapstructure:"algorithm"`
	UseTLS             bool           `mapstructure:"useTLS"`
	VerifySSL          bool           `mapstructure:"verifySSL"`
}

// StructFunction :
type StructFunction struct {
	HandlerName  interface{}
	FunctionName string
}
