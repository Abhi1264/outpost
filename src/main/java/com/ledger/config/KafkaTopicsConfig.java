package com.ledger.config;

import com.ledger.webhook.WebhookProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
class KafkaTopicsConfig {

	@Bean
	KafkaAdmin.NewTopics webhookTopics(WebhookProperties properties) {
		return new KafkaAdmin.NewTopics(
				TopicBuilder.name(properties.dispatchTopic()).partitions(3).build(),
				TopicBuilder.name(properties.dlqTopic()).partitions(1).build());
	}

}
