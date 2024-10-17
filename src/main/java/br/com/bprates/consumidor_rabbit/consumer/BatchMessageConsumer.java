package br.com.bprates.consumidor_rabbit.consumer;

import br.com.bprates.consumidor_rabbit.config.RabbitMQConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.stereotype.Service;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class BatchMessageConsumer implements ChannelAwareMessageListener {
    private static final Logger logger = LoggerFactory.getLogger(BatchMessageConsumer.class);
    private static final int BATCH_SIZE = 100;
    private List<Message> messageBuffer = Collections.synchronizedList(new ArrayList<>());

    @Override
    @RabbitListener(queues = RabbitMQConfig.QUEUE_NAME)
    public void onMessage(Message message, Channel channel) throws Exception {
        messageBuffer.add(message);

        if (messageBuffer.size() >= BATCH_SIZE) {
            processBatch(messageBuffer);
            messageBuffer.clear();
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), true);
        }
    }

    private void processBatch(List<Message> messages) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<ClienteCriadoEvento> eventos = new ArrayList<>();

        for (Message message : messages) {
            try {
                String body = new String(message.getBody(), StandardCharsets.UTF_8);
                ClienteCriadoEvento evento = objectMapper.readValue(body, ClienteCriadoEvento.class);
                eventos.add(evento);
            } catch (Exception e) {
                logger.error("Erro ao converter mensagem: {}", e.getMessage(), e);
            }
        }
    }
}

