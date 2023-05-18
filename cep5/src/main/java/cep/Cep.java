package cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.Path;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Cep {

	public static void main(String[] args) throws Exception {

		// flink environment init
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1); // ensure ordering of record stream

		// reference to dir containing station pollution data
		String stationDataDir = "data";

		// file delimiter for the csv files [CO, Interval]
		String fileDelimiter = ",";

		// set up a flink source to monitor the station data dir
		final FileSource<String> source =
				FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(stationDataDir))
						.monitorContinuously(Duration.ofSeconds(1L))
						.build();

		// read files from the dir and monitor for new ones
		final DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

		// transform data stream to pollution object
		DataStream<Pollution> pollutionDataStream = dataStream
				.map((MapFunction<String, Pollution>) record -> {

					try {
						String[] op = record.split(fileDelimiter);
						return new Pollution(Double.parseDouble(op[1]), Long.parseLong(op[0]));
					} catch (Exception ignored) {
						return new Pollution(0, 0);
					}
				});

		// pattern setup for filtering low pollution events
		double lowPollutionThreshold = 200;
		PatternStream<Pollution> observationPatternStream = CEP.pattern(
						pollutionDataStream,
						Pattern.<Pollution>begin("uno")
								.where(new IterativeCondition<Pollution>() {
									@Override
									public boolean filter(Pollution event, Context<Pollution> context) {
										return event.getCarbonMonoxide() < lowPollutionThreshold;
									}
								}))
				.inProcessingTime();

		// process to create a custom alert
		DataStream<String> alertDataStream = observationPatternStream.process(
				new PatternProcessFunction<Pollution, String>() {

					@Override
					public void processMatch(Map<String, List<Pollution>> map, Context context, Collector<String> collector) {
						String alert = "Low pollution alert: " +  map.get("uno").get(0).getCarbonMonoxide();
						collector.collect(alert);
					}
				});

		// default print to console
		alertDataStream.print();

		env.execute("Pollution Events Pattern");
	}
}
