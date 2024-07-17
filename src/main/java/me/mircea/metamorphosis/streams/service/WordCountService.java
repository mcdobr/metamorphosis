package me.mircea.metamorphosis.streams.service;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Objects;

@Service
@RequiredArgsConstructor
@Slf4j
public class WordCountService {
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final String DOCUMENT_TOPIC = "documentTopic";
    private static final String WORD_COUNT_TOPIC = "wordCountTopic";
    private static final String WORD_SEPARATOR_REGEX = "\\W+";
    private static final String COUNTS_STORE = "counts";

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> documentStream = streamsBuilder.stream(
                DOCUMENT_TOPIC,
                Consumed.with(STRING_SERDE, STRING_SERDE)
        );

        KTable<String, Long> wordCounts = documentStream
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(value -> Arrays.asList(value.split(WORD_SEPARATOR_REGEX)))
                .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
                .count(Materialized.as(COUNTS_STORE));

        wordCounts.toStream().to(WORD_COUNT_TOPIC);
    }

    public void ingestMessage(String message) {
        kafkaTemplate.send(DOCUMENT_TOPIC, message)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Message sent to topic: {}", message);
                    } else {
                        log.warn("Failed to send message", ex);
                    }
                });
    }

    public Long countAppearances(@NonNull String word) {
        KafkaStreams kafkaStreams = Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams());
        ReadOnlyKeyValueStore<String, Long> countsStore = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(COUNTS_STORE, QueryableStoreTypes.keyValueStore())
        );
        return countsStore.get(word);
    }
}
