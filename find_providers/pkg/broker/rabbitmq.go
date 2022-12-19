package broker

import "github.com/streadway/amqp"

// consumeRabbitMq consumes messages from a RabbitMQ broker
func consumeRabbitMq(msgs <-chan amqp.Delivery, logCh chan string) {
	for m := range msgs {
		logCh <- string(m.Body)
	}
}

// prepareRabbitMq prepares a connection to a RabbitMQ broker
func prepareRabbitMq(rabbitmqHost, groupId string) <-chan amqp.Delivery {
	conn, err := amqp.Dial(rabbitmqHost)
	if err != nil {
		panic(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	q, err := ch.QueueDeclare(
		groupId,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	return msgs
}
