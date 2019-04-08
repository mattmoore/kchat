package io.mattmoore.kchat.view

import javafx.beans.property.SimpleStringProperty
import javafx.collections.FXCollections
import javafx.scene.input.KeyCode
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import tornadofx.*
import java.time.Duration
import java.util.*

class MainView : View("KChat") {
    val messageHistory: MessageHistoryController by inject()
    val message = SimpleStringProperty()
    val producer = createProducer()
    val consumer = createConsumer()

    override val root = form {
        listview(messageHistory.values) {
        }

        fieldset {
            field {
                textfield(message) {
                    setOnKeyPressed {
                        if (it.code == KeyCode.ENTER) sendMessage(message.value)
                    }
                }

                button("Send") {
                    action {
                        sendMessage(message.value)
                    }
                }
            }
        }
    }

    init {
        with(root) {
            prefHeight = 600.0
            prefWidth = 800.0
            consumeMessages(consumer, messageHistory)
        }
    }

    private fun createProducer(): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["key.serializer"] = StringSerializer::class.java.canonicalName
        props["value.serializer"] = StringSerializer::class.java.canonicalName
        return KafkaProducer<String, String>(props)
    }

    private fun createConsumer(): KafkaConsumer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["group.id"] = "message-processor"
        props["key.deserializer"] = StringDeserializer::class.java
        props["value.deserializer"] = StringDeserializer::class.java
        return KafkaConsumer(props)
    }

    private fun consumeMessages(consumer: KafkaConsumer<String, String>, messageHistory: MessageHistoryController) {
        val messageList = mutableListOf<String>()
        consumer.subscribe(listOf("messages"))
        runAsync {
            while (true) {
                val records = consumer.poll(Duration.ofSeconds(1))
                if (records.count() == 0) continue
                records.forEach {
                    messageList.add(it.value())
                }
            }
        } ui {
            messageList.forEach {
                messageHistory.values.add(it)
            }
        }
    }

    private fun sendMessage(msg: String) {
        messageHistory.values.add(msg)
        message.value = ""
        producer.send(ProducerRecord("messages", msg))
    }
}

class MessageHistoryController: Controller() {
    val values = FXCollections.observableArrayList<String>()
}
