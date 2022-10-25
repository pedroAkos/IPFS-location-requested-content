package broker

import log "github.com/sirupsen/logrus"

func PrepareBroker(broker_to_use, host, topic string) chan string {

	logch := make(chan string)
	switch broker_to_use {
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
