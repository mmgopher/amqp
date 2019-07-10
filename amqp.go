package rabbit

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"main/src/config"
	"sync"
	"sync/atomic"
	"time"
)

var ErrConnectionClosed = errors.New("failed to publish message")

const (
	confirmationDelay      = 5 * time.Second
	reconnectDelay         = 3 * time.Second
	publishConnectionDelay = 500 * time.Millisecond
	maxReconnectAttempts   = 3
)

type AmqpClient struct {
	connection    *amqp.Connection
	channel       *amqp.Channel
	notifyClose   chan *amqp.Error
	notifyConfirm chan amqp.Confirmation
	closed        chan struct{}
	once          sync.Once
	url           string
	exchange      string
	exchangeType  string
	routingKey    string
	isConnected   atomic.Value
}

func NewAmqpClient() *AmqpClient {

	ampqConfig := config.GetConfiguration().Ampq
	client := AmqpClient{
		url:          fmt.Sprintf("amqp://%s:%s@%s:%s", ampqConfig.User, ampqConfig.Password, ampqConfig.Host, ampqConfig.Port),
		exchange:     ampqConfig.Exchange,
		exchangeType: ampqConfig.ExchangeType,
		routingKey:   ampqConfig.RoutingKey,
		closed:       make(chan struct{}),
	}
	client.isConnected.Store(false)
	go client.reconnect()
	return &client

}

func (ac *AmqpClient) reconnect() {
	for {
		ac.isConnected.Store(false)
		reconnectAttempts := 0
		for err := ac.connect(); err != nil; err = ac.connect() {
			reconnectAttempts++
			log.Info("Trying to reconnect to RabbitMQ")
			if reconnectAttempts >= maxReconnectAttempts {
				log.Error("Can not connect to RabbitMQ. Give up reconnecting", err)
				ac.Close()
				return
			}
			time.Sleep(reconnectDelay)

		}

		select {
		case <-ac.closed:
			return
		case <-ac.notifyClose:
		}
	}
}

func (ac *AmqpClient) connect() error {
	log.Debug("I'm creating a connection to RabbitMQ")

	conn, err := amqp.DialConfig(ac.url,
		amqp.Config{
			Heartbeat: time.Second,
			Locale:    "en_US",
		},
	)
	if err != nil {
		log.Warn("Error while connection creation")
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Warn("Error while channel creation")
		conn.Close()
		return err
	}
	ch.Confirm(false)
	err = ch.ExchangeDeclare(
		ac.exchange,
		ac.exchangeType,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Warn("Can't create an exchange", err)
		ch.Close()
		conn.Close()
		return err
	}

	ac.setupConnection(conn, ch)
	ac.isConnected.Store(true)
	log.Debug("Connection established")
	return nil
}
func (ac *AmqpClient) setupConnection(connection *amqp.Connection, channel *amqp.Channel) {
	ac.connection = connection
	ac.channel = channel
	ac.notifyConfirm = make(chan amqp.Confirmation)
	ac.notifyClose = make(chan *amqp.Error)
	ac.channel.NotifyPublish(ac.notifyConfirm)
	ac.channel.NotifyClose(ac.notifyClose)

}
func (ac *AmqpClient) Close() {

	ac.once.Do(func() {
		ac.isConnected.Store(false)
		close(ac.closed)
		if ac.channel != nil {
			ac.channel.Close()
			ac.channel = nil
		}

		if ac.connection != nil {
			ac.connection.Close()
			ac.connection = nil
		}

	})
}

func (ac *AmqpClient) Push(body []byte) error {

	for !ac.isConnected.Load().(bool) {
		select {
		case <-ac.closed:
			return ErrConnectionClosed

		case <-time.After(publishConnectionDelay):

		}
	}

	if len(body) == 0 {
		return errors.New("can't publish an empty message")
	}

	err := ac.publish(body)
	if err != nil {
		log.Error("Publishing message failed")
		return err
	}

	select {
	case confirm := <-ac.notifyConfirm:
		if confirm.Ack {
			log.Debug("Push confirmed!")
			return nil
		}
	case <-time.After(confirmationDelay):
	}

	return errors.New("can't confirm message push")
}

func (ac *AmqpClient) publish(body []byte) error {
	log.Info("Push message to Rabbit queue")
	log.Debug("Message body: " + string(body))

	return ac.channel.Publish(
		ac.exchange,
		ac.routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}
