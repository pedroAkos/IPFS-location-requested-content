package broker

import log "github.com/sirupsen/logrus"

// PrepareBroker prepares a broker for reading
func PrepareBroker(brokerToUse, host, topic string) chan string {

	logch := make(chan string)
	switch brokerToUse {
	case "kafka":
		log.Debug("Preparing kafka broker..")
		//consumer := prepareKafka()
		//consumeKafkaLog(consumer, logch)
	case "rabbitmq":
		log.Debug("Preparing rabbitmq broker..")
		msgs := prepareRabbitMq(host, topic)
		go consumeRabbitMq(msgs, logch)

	}

	return logch
}
