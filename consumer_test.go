package test_consumer

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

func TestConsumer(t *testing.T) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	assert.Nil(t, err)
	defer client.Close()

	// this is how the example test consumer creates a topic
	// topic := fmt.Sprintf("my-topic-%d", time.Now().Unix())

	// if I try to re-use the same topic everytime, it will fail after the first run
	// unless I run the following command line: pulsar-client consume --subscription-name my-sub  --num-messages 0 my-topic
	topic := fmt.Sprintf("my-topic")

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})

	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:             topic,
		SubscriptionName:  "my-sub",
		AckTimeout:        1 * time.Minute,
		Name:              "my-consumer-name",
		ReceiverQueueSize: 100,
		MaxTotalReceiverQueueSizeAcrossPartitions: 10000,
		Type: pulsar.Exclusive,
	})

	assert.Nil(t, err)
	defer consumer.Close()

	assert.Equal(t, consumer.Topic(), "persistent://public/default/"+topic)
	assert.Equal(t, consumer.Subscription(), "my-sub")

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		sendTime := time.Now()
		if err := producer.Send(ctx, pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}

		msg, err := consumer.Receive(ctx)
		recvTime := time.Now()
		assert.Nil(t, err)
		assert.NotNil(t, msg)

		assert.Equal(t, string(msg.Payload()), fmt.Sprintf("hello-%d", i))
		assert.Equal(t, msg.Topic(), "persistent://public/default/"+topic)
		fmt.Println("Send time: ", sendTime)
		fmt.Println("Publish time: ", msg.PublishTime())
		fmt.Println("Receive time: ", recvTime)
		assert.True(t, sendTime.Unix() <= msg.PublishTime().Unix())
		assert.True(t, recvTime.Unix() >= msg.PublishTime().Unix())

		serializedId := msg.ID().Serialize()
		deserializedId := pulsar.DeserializeMessageID(serializedId)
		assert.True(t, len(serializedId) > 0)
		assert.True(t, bytes.Equal(deserializedId.Serialize(), serializedId))

		consumer.Ack(msg)
	}

	err = consumer.Seek(pulsar.EarliestMessage)
	assert.Nil(t, err)

	err = consumer.Unsubscribe()
	assert.Nil(t, err)

	t.Log("if it's failing run: pulsar-client consume --subscription-name my-sub  --num-messages 0 my-topic")
}
