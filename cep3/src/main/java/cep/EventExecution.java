package cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class EventExecution {

    public void execute() throws Exception {
        // flink application entry env
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<PollutionEvent> eventDataStream1 = environment.fromCollection(processFileToPollutionEvent("pollutionData184675.csv", "Arhus1" ));
        DataStream<PollutionEvent> eventDataStream2 = environment.fromCollection(processFileToPollutionEvent("pollutionData1846752.csv", "Arhus2"));

        DataStream<PollutionEvent> events = eventDataStream1.union(eventDataStream2);

        // setting up the flink pattern
        PatternStream<PollutionEvent> patternStream = CEP
                .pattern(
                        events.keyBy(PollutionEvent::getStation),
                        Pattern.<PollutionEvent>begin("one")
                                .where(new IterativeCondition<PollutionEvent>() {
                                    @Override
                                    public boolean filter(PollutionEvent event, Context<PollutionEvent> context) {
                                        return event.getCarbon_monoxide() < 100;
                                    }
                                }))
                .inProcessingTime();

        // process the pattern stream event
        DataStream<String> result = patternStream.select((PatternSelectFunction<PollutionEvent, String>) map -> {
            PollutionEvent event = map.get("one").get(0);
            int carbon = event.getCarbon_monoxide();
            String station = event.getStation();
            return "Pollution Alert: " + station + ", carbon: " + carbon;
        });

        result.print();

        environment.execute();
    }

    List<PollutionEvent> processFileToPollutionEvent(String filename, String station) throws IOException {

        File file = new File(filename);
        InputStream in = Files.newInputStream(file.toPath());
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        List<PollutionEvent> pollutionEvents = new ArrayList<>();
        String line;

        while ((line = br.readLine()) != null) {
            try
            {
                // create new events
                PollutionEvent pollutionEvent = new PollutionEvent(station, Integer.parseInt(line.split(",")[1]));
                pollutionEvents.add(pollutionEvent);
            } catch (NumberFormatException ignored) {
            }
        }

        return pollutionEvents;
    }
}
