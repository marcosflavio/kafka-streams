package com.marcosflavio.kafka.pipe;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class DollarConversorPipe {

	private static final Double REAL_FACTOR = 5.10;

	public static void run(Properties properties, String input, String output) {
		final StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> dollarConversorInput = builder.stream(input);

		KStream<String, Double> convertedDollarKStream = dollarConversorInput
				.flatMapValues(value -> Arrays.asList(convertToReal(value)));
		convertedDollarKStream.to(output, Produced.with(Serdes.String(), Serdes.Double()));

		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, properties);
		final CountDownLatch latch = new CountDownLatch(1);

		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}

		System.exit(0);
	}

	private static Double convertToReal(String value) {
		try {
			return Double.valueOf(value) * REAL_FACTOR;
		} catch (Exception e) {
		}
		return 0.0;
	}
}
