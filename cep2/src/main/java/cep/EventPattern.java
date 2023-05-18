package cep;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.io.File;

public class EventPattern {

	// this method processes the csv file and maps it to the custom event stream object
	DataStream<StationEvent> processFile(StreamExecutionEnvironment environment,
										 String stationName,
										 String filename) {

		// get file path
		String filepath = (new File(getClass().getClassLoader().getResource(filename).getFile())).getAbsolutePath();

		TextInputFormat inputFormat = new TextInputFormat(new Path(filepath));
		inputFormat.setCharsetName("UTF-8");

		// initial read of the file with each line processed as a string
		DataStreamSource<String> dataStreamSource = environment.readFile(inputFormat, filepath,
				FileProcessingMode.PROCESS_ONCE, 60000L, BasicTypeInfo.STRING_TYPE_INFO);

		// the string is processed to the custom event stream
		DataStream<StationEvent> dataStream = dataStreamSource.map(
				(MapFunction<String, StationEvent>) s -> {

					String[] line = s.split(",");

					long timeInterval;
					int carbonMonoxide;

					try {
						// 1 second wait
						TimeUnit.SECONDS.sleep(1);

						// parse the line item
						carbonMonoxide = Integer.parseInt(line[0]);
						timeInterval = Long.parseLong(line[1]);

						// create event sensor object
						return new StationEvent(stationName, carbonMonoxide, timeInterval);
					} catch (NumberFormatException e) {
						// use a default
						return new StationEvent("", 0, 0);
					}
		});

		return dataStream;
	}

	public JobExecutionResult execute() throws Exception {
		// application entry for flink environment
		final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		// station names
		String station1 = "Arhus1";
		String station2 = "Aarhus2";

		// document accessible in resources
		String filename1 = "pollutionData184675.csv";
		String filename2 = "pollutionData184675.csv";

		// process the files
		DataStream<StationEvent> stationEventDataStream1 = processFile(environment, station1, filename1);
//		stationEventDataStream1.print();

		DataStream<StationEvent> stationEventDataStream2 = processFile(environment, station2, filename2);
//		stationEventDataStream2.print();

		// set time intervals with flink watermark
		stationEventDataStream1.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<StationEvent>() {
			public long extractAscendingTimestamp(StationEvent event) {
				return event.getTimeInterval();
			}
		});

		stationEventDataStream2.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<StationEvent>() {
			public long extractAscendingTimestamp(StationEvent stationEvent) {
				return stationEvent.getTimeInterval();
			}
		});

		// pattern definition
		Pattern<StationEvent, StationEvent> stationEventPattern = Pattern.<StationEvent>begin("start")
				.where(new IterativeCondition<StationEvent>() {
					@Override
					public boolean filter(StationEvent event, Context<StationEvent> context) {
						return event.getCarbonMonoxide() > 50 && !event.getStationId().isEmpty();
					}
				});

		// setting up the flink pattern to detect from the first stream
		PatternStream<StationEvent> stationEventPatternStream1 = CEP.pattern(stationEventDataStream1.keyBy(StationEvent::getStationId), stationEventPattern).inProcessingTime();

		// setting up the flink pattern to detect from the second stream
		PatternStream<StationEvent> stationEventPatternStream2 = CEP.pattern(stationEventDataStream2.keyBy(StationEvent::getStationId), stationEventPattern).inProcessingTime();

		// define a process for the station low pollution alert
		PatternProcessFunction<StationEvent, String> patternProcessFunction = new PatternProcessFunction<StationEvent, String>() {
			@Override
			public void processMatch(Map<String, List<StationEvent>> map, Context context, Collector<String> collector) {
				int co = map.get("start").get(0).getCarbonMonoxide();
				String stationName = map.get("start").get(0).getStationId();

				collector.collect("Station alert - CO: " + co + ", Name: " + stationName);
			}
		};

		// integrate the process on the pattern stream and print to out
		stationEventPatternStream1.process(patternProcessFunction).print();
		stationEventPatternStream2.process(patternProcessFunction).print();

		return environment.execute("Low pollution event");
	}


	public static void main(String[] args) throws Exception {

		EventPattern eventPattern = new EventPattern();

		// Execute the flink application
		eventPattern.execute();
	}


	// custom class to define each reading from the file
	public static class StationEvent {

		private String stationId;
		private int carbonMonoxide;
		private long timeInterval;

		public StationEvent(String stationId, int carbonMonoxide, long timeInterval)
		{
			this.stationId = stationId;
			this.carbonMonoxide = carbonMonoxide;
			this.timeInterval = timeInterval;
		}

		public String getStationId() {
			return stationId;
		}

		public void setStationId(String stationId) {
			this.stationId = stationId;
		}
		public int getCarbonMonoxide() {
			return carbonMonoxide;
		}

		public void setCarbonMonoxide(int carbonMonoxide) {
			this.carbonMonoxide = carbonMonoxide;
		}

		public long getTimeInterval() {
			return timeInterval;
		}

		public void setTimeInterval(long timeInterval) {
			this.timeInterval = timeInterval;
		}
	}
}
