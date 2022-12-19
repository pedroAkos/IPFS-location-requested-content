package broker

// This is left commented because we were unable to containerize the Kafka client library

//func prepareKafka() *kafka.Consumer {
//	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
//		"bootstrap.servers":    bootstrap_servers,
//		"group.id":             group_id,
//		"max.poll.interval.ms": max_poll_interval_ms,
//		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "smallest"}})
//
//	if err != nil {
//		panic(err)
//	}
//
//	err = consumer.Subscribe("logs", nil)
//	if err != nil {
//		panic(err)
//
//	}
//
//	return consumer
//}

//func consumeKafkaLog(consumer *kafka.Consumer, logCh chan string) {
//	run := true
//	for run == true {
//		ev := consumer.Poll(10000)
//		switch e := ev.(type) {
//		case *kafka.Message:
//			logCh <- string(e.Value)
//		case kafka.Error:
//			_, _ = fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
//			run = false
//		default:
//			//fmt.Printf("Ignored %v\n", e)
//		}
//	}
//}
