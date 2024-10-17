package br.com.bprates.consumidor_rabbit.service;

import br.com.bprates.consumidor_rabbit.config.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.stream.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class BatchMessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BatchMessageConsumer.class);

    @RabbitListener(queues = RabbitMQConfig.QUEUE_NAME, containerFactory = "batchContainerFactory")
    public void receiveMessages(List<Message> messages, Channel channel) {
        try {
            logger.info("Recebido lote de {} mensagens", messages.size());

            // Implementar as regras de negócio aqui
            processBatch(messages);

            // Confirma todas as mensagens processadas
            long lastDeliveryTag = messages.get(messages.size() - 1).getMessageProperties   ().getDeliveryTag();
            channel.basicAck(lastDeliveryTag, true);

            logger.info("Lote processado com sucesso.");
        } catch (Exception e) {
            logger.error("Erro ao processar o lote: {}", e.getMessage(), e);
            // Tratar falhas, possivelmente rejeitando as mensagens
            messages.forEach(message -> {
                try {
                    channel.basicReject(message.getMessageProperties().getDeliveryTag(), true);
                } catch (IOException ioException) {
                    logger.error("Erro ao rejeitar mensagem: {}", ioException.getMessage(), ioException);
                }
            });
        }
    }

    private void processBatch(List<Message> messages) {
        // Sua lógica de processamento em lote
    }
}
