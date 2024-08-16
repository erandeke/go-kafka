package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `from : "text" json:"text"`
}

func main() {

	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)
	app.Listen(":3000")
}

func createComment(c *fiber.Ctx) error {
	// In fibre the request body is sent through the context
	cmt := new(Comment) // Inbuilt function that allocates the memory
	//body parser binds the request into struct for fiber
	if err := c.BodyParser(cmt); err != nil {
		log.Println("Error")
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err

	}

	// Push the bytes to Kafka
	cmtInBytes, err := json.Marshal(cmt)

	// push to kafka
	PushCommentsToKafka("comments", cmtInBytes)

	err = c.JSON(&fiber.Map{
		"success":  true,
		"comments": cmt,
		"message":  "Message posted successfully",
	})

	if err != nil {
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": "Message cannot be produced due to err",
		})
		return err
	}

	return err

}

func PushCommentsToKafka(topic string, message []byte) error {

	brokersUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil

}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
