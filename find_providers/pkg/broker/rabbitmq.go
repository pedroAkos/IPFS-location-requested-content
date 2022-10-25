package broker

import "github.com/streadway/amqp"

func consumeRabbitMq(msgs <-chan amqp.Delivery, logch chan string) {
	for m := range msgs {
		logch <- string(m.Body)
	}
}

func prepareRabbitMq(rabbitmq_host, group_id string) <-chan amqp.Delivery {
	conn, err := amqp.Dial(rabbitmq_host)
	if err != nil {
		panic(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	q, err := ch.QueueDeclare(
		group_id,
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
