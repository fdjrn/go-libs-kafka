package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type ScramConfig struct {
	EnableStatus bool
	User         string
	Password     string
	Mechanism    string
	UseTLS       bool
	VerifySSL    bool
}

func (s *ScramConfig) GenerateScramConfig(kafkaConfig *sarama.Config) error {
	if s.EnableStatus {
		kafkaConfig.Net.SASL.Enable = s.EnableStatus
		kafkaConfig.Net.SASL.User = s.User
		kafkaConfig.Net.SASL.Password = s.Password
		kafkaConfig.Net.SASL.Handshake = true

		if s.Mechanism == "" {
			return fmt.Errorf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", s.Mechanism)
		}

		if s.Mechanism == "sha512" {
			kafkaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		}

		if s.Mechanism == "sha256" {
			kafkaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		}

		if s.UseTLS {
			kafkaConfig.Net.TLS.Enable = true
			// kafkaConfig.Net.TLS.Config = &tls.Config{InsecureSkipVerify: false}
		}

	}
	return nil
}
