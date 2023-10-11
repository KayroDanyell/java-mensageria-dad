// Kayro Danyell Alves - 1381452
// Wagner Cipriano da Silva - Desenvolvimento de Aplicações Distribuidas
// Atividade de mensageria com kafka
package org.example;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "dad-mensageria.servicebus.windows.net:9093");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" " +
                "password=\"Endpoint=sb://kayro-dad.servicebus.windows.net/;SharedAccessKeyName=kayroDad;SharedAccessKey=+dau2ikplIAJcp7zcUMFItso1KeY76XZ7+AEhA4qwUw=;EntityPath=kayro-dad-kafka\";");

        Producer<String, String> producer = new KafkaProducer<>(props,new StringSerializer(), new StringSerializer());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        try {
            String messageJson = objectMapper.writeValueAsString(new Message("Kayro Danyell Alves", "1381452@sga.pucminas.br", 3));
            ProducerRecord<String, String> record = new ProducerRecord<>("kayro-dad", messageJson);
            producer.send(record, (metadata, exception) -> {
                if (metadata != null) {
                    System.out.println("Mensagem enviada com sucesso para o tópico: " + metadata.topic());
                } else {
                    System.err.println("Erro ao enviar mensagem: " + exception.getMessage());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}