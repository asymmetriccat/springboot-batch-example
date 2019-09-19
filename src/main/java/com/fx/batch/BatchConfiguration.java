package com.fx.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fx.batch.listener.JobCompletionNotificationListener;
import com.fx.batch.model.FxMarketEvent;
import com.fx.batch.model.FxMarketPricesStore;
import com.fx.batch.model.Trade;
import com.fx.batch.processor.FxMarketEventProcessor;
import com.fx.batch.reader.FxMarketEventReader;
import com.fx.batch.writer.StockPriceAggregator;

/**
 * The Class BatchConfiguration.
 *
 */
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public FxMarketPricesStore fxMarketPricesStore() {
		return new FxMarketPricesStore();
	}

	// FxMarketEventReader (Reader)
	@Bean
	public FxMarketEventReader fxMarketEventReader() {
		return new FxMarketEventReader();
	}

	// FxMarketEventProcessor (Processor)
	@Bean
	public FxMarketEventProcessor fxMarketEventProcessor() {
		return new FxMarketEventProcessor();
	}

	// StockPriceAggregator (Writer)
	@Bean
	public StockPriceAggregator stockPriceAggregator() {
		return new StockPriceAggregator();
	}

	// JobCompletionNotificationListener (File loader)
	@Bean
	public JobExecutionListener listener() {
		return new JobCompletionNotificationListener();
	}

	// Configure job step
	@Bean
	public Job fxMarketPricesETLJob() {
		return jobBuilderFactory.get("FxMarket Prices ETL Job").incrementer(new RunIdIncrementer()).listener(listener())
				.flow(etlStep()).end().build();
	}

	@Bean
	public Step etlStep() {
		return stepBuilderFactory.get("Extract -> Transform -> Aggregate -> Load").<FxMarketEvent, Trade> chunk(10000)
				.reader(fxMarketEventReader()).processor(fxMarketEventProcessor())
				.writer(stockPriceAggregator())
				.build();
	}

}
