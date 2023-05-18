package cep;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.CEP;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;


public class CepApplication {


	public static void main(String[] args) throws Exception {

		System.out.println("-----------------Program Start-------------------");

		// Setting up environment for flink application entry
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Referencing files from the resources dir
		String filepath1 = "pollutionData184675.csv";
		String stationId1 = "Aarhus1";
		String filepath2 = "pollutionData184675.csv";
		String stationId2 = "Aarhus2";

		// Setting up the custom timestamp from the carbon monoxide event
		AscendingTimestampExtractor timestampExtractor = new AscendingTimestampExtractor<EventSensor>() {
			public long extractAscendingTimestamp(EventSensor eventSensor) {
				return eventSensor.getTimestamp();
			}
		};

		// Setting up input stream generated from the content of the csv files
		StationDataStream stationDataStream1 = new StationDataStream(filepath1, stationId1);
		StationDataStream stationDataStream2 = new StationDataStream(filepath2, stationId2);

		DataStream<EventSensor> inputStream = env.addSource(stationDataStream1)
				.assignTimestampsAndWatermarks(timestampExtractor);

		DataStream<EventSensor> inputStream2 = env.addSource(stationDataStream2)
				.assignTimestampsAndWatermarks(timestampExtractor);

		// Combining the data from the previous inputs
		DataStream<EventSensor> combined = inputStream.connect(inputStream2).flatMap(
				new RichCoFlatMapFunction<EventSensor, EventSensor, EventSensor>() {
					@Override
					public void flatMap1(EventSensor eventSensor, Collector<EventSensor> collector) throws Exception {
						collector.collect(eventSensor);
					}

					@Override
					public void flatMap2(EventSensor eventSensor, Collector<EventSensor> collector) throws Exception {
						collector.collect(eventSensor);
					}
				}
		);

		// combined.print(); // used for debugging purpose

		// we define a pattern to detect carbon monoxide below a specified threshold
		double THRESHOLD = 55;
		Pattern<EventSensor, EventSensor> lowCarbonMonoxidePattern = Pattern.<EventSensor>begin("first")
				.where(new IterativeCondition<EventSensor>() {
					@Override
					public boolean filter(EventSensor sensor, Context<EventSensor> context) {
						return sensor.getCarbonMonoxide() < THRESHOLD;
					}
				});

		// setting up the flink pattern to detect from the combined stream
		PatternStream<EventSensor> eventSensorPatternStream = CEP.pattern(combined.keyBy(EventSensor::getStationName), lowCarbonMonoxidePattern);

		// setting up a flink process to generate alert on pattern detection
		DataStream<String> result = eventSensorPatternStream.process(new PatternProcessFunction<EventSensor, String>() {
			@Override
			public void processMatch(Map<String, List<EventSensor>> map, Context context, Collector<String> collector) {
				collector.collect(String.format("Low CO Alert: %f, Station Name: %s", map.get("first").get(0).getCarbonMonoxide(), map.get("first").get(0).getStationName()));
			}
		});

		// the alert is printed to std out
		result.print();

		env.execute("Flink Application for detecting pollution");
	}

	// This class extends a flink SourceFunction to stream the data from the csv file
	public static class StationDataStream extends RichSourceFunction<EventSensor> {
		private boolean working = true;
		final private String filepath;
		final private String name;

		public StationDataStream(String filepath, String name) {
			this.filepath = filepath;
			this.name = name;
		}

		@Override
		public void run(SourceContext<EventSensor> sourceContext) throws Exception {
			try {
				File file = new File(Objects.requireNonNull(getClass().getClassLoader().getResource(filepath)).getFile());
				Scanner scanner = new Scanner(file);
				String line;
				int lineNumber = 1;
				while (scanner.hasNextLine() && working) {
					// read next line and remove whitespaces
					line = scanner.nextLine().replaceAll("//s", "");
					String[] values = line.split(",");

					long timestamp;
					double carbonMonoxide;

					try {
						timestamp = Long.parseLong(values[1]);
						carbonMonoxide = Long.parseLong(values[0]);
					} catch (Exception e) {
						System.out.printf("%s - Error with line number: %d\n", name, lineNumber);
						lineNumber++;
						// cancel();
						continue;
					}

					// create event sensor with read details
					EventSensor eventSensor = new EventSensor(name, carbonMonoxide, timestamp);

					// put generated sensor data to the queue
					sourceContext.collect(eventSensor);

					// simulate delay for two seconds
					TimeUnit.SECONDS.sleep(2);

					// increment number of lines processed
					lineNumber++;
				}
				System.out.printf("%s - Number of lines processed: %d\n", name, lineNumber);

			} catch (FileNotFoundException e) {
				System.out.printf("%s - File not found: %s\n", name, e);
			} catch (Exception e) {
				System.out.printf("%s - Error: %s\n", name, e);
			}
		}

		@Override
		public void cancel() {
			this.working = false;
		}

	}
}
