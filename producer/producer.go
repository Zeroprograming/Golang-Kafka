package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

// Comment struct
type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {

	// Api setup
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)
	app.Listen(":3000")

}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	// Create new Kafka producer instance with sarama
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	// Connect to Kafka
	// BrokersUrl is a list of addresses for Kafka brokers
	// config is the Kafka configuration
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	// Return Kafka producer connection
	return conn, nil
}

// PushCommentToQueue pushes comment to queue
func PushCommentToQueue(topic string, message []byte) error {

	// BrokersUrl is a list of addresses for Kafka brokers
	brokersUrl := []string{"localhost:29092"}

	// Connect to Kafka
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	// Close Kafka producer connection
	defer producer.Close()

	// Create new Kafka message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	// Send message to Kafka
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	// Print message information
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	// Return nil
	return nil
}

// createComment handler
func createComment(c *fiber.Ctx) error {

	// Instantiate new Message struct
	cmt := new(Comment)

	//  Parse body into comment struct
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}

	// Convert comment struct to JSON
	cmtInBytes, _ := json.Marshal(cmt)
	PushCommentToQueue("comments", cmtInBytes)

	// Return Comment in JSON format
	err := c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error creating product",
		})
		return err
	}

	return err
}
