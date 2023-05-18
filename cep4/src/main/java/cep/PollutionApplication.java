package cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

import java.util.List;
import java.util.Map;

public class PollutionApplication {

	public static void main(String[] args) throws Exception {

		// flink application entry env
		final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		// keeps ordering of records
		environment.setParallelism(1);

		//Define the data directories to monitor for new files
		String station1DataDir = "data/aarhus1";
		String station2DataDir = "data/aarhus2";

		//Create a DataStream based on the directory
		DataStream<String> stream1
				= environment.readFile(new TextInputFormat(new Path(station1DataDir)),
				station1DataDir,
				FileProcessingMode.PROCESS_CONTINUOUSLY,
				1000); //monitor interval

		//Create a DataStream based on the directory
		DataStream<String> stream2
				= environment.readFile(new TextInputFormat(new Path(station2DataDir)),
				station2DataDir,
				FileProcessingMode.PROCESS_CONTINUOUSLY,
				1000); //monitor interval

		//Convert each record to an Object
		DataStream<Observation> record1
				= stream1
				.map(new MapFunction<String,Observation>() {
					@Override
					public Observation map(String record) {
						String[] op = record.split(",");

						try {
							String stationName = "Aarhus1";
							int co = Integer.parseInt(op[1]);
							long timeInterval = Long.parseLong(op[0]);

							return new Observation(stationName, co, timeInterval);
						} catch (NumberFormatException ignored) {
							return new Observation("", 0, 0);
						}
					}
				});

		//Convert each record to an Object
		DataStream<Observation> record2
				= stream2
				.map(new MapFunction<String,Observation>() {
					@Override
					public Observation map(String record) {
						String[] op = record.split(",");

						try {
							String stationName = "Aarhus2";
							int co = Integer.parseInt(op[1]);
							long timeInterval = Long.parseLong(op[0]);

							return new Observation(stationName, co, timeInterval);
						} catch (NumberFormatException ignored) {
							return new Observation("", 0, 0);
						}
					}
				});

		// combine streams from the two data dirs
		DataStream<Observation> observationDataStream = record1.union(record2);

		// get observations by station ids
		KeyedStream<Observation, String> keyedStream = observationDataStream.keyBy(Observation::getId);

		// setup pattern for observations
		PatternStream<Observation> observationPatternStream = CEP.pattern(
						keyedStream,
						Pattern.<Observation>begin("start")
								.where(new IterativeCondition<Observation>() {
									@Override
									public boolean filter(Observation event, Context<Observation> context) {
										return event.getCarbonMonoxide() < 120;
									}
								}))
				.inProcessingTime();

		// process the pattern to generate a flink alert
		DataStream<Alert> alertDataStream = observationPatternStream.process(new PatternProcessFunction<Observation, Alert>() {
			@Override
			public void processMatch(Map<String, List<Observation>> map, Context context, Collector<Alert> collector) {
				Alert alert = new Alert();
				alert.setId(map.get("start").get(0).getCarbonMonoxide());
				collector.collect(alert);
			}
		});

		// output to std out
		alertDataStream.print();

		environment.execute("Low pollution Tracking");
	}
}
