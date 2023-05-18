package cep;

public class EventSensor {

    private String stationName;
    private double carbonMonoxide;
    private long timestamp;

    public EventSensor(String stationName, double carbonMonoxide, long timestamp)
    {
        this.stationName = stationName;
        this.carbonMonoxide = carbonMonoxide;
        this.timestamp = timestamp;
    }

    public double getCarbonMonoxide() {
        return carbonMonoxide;
    }

    public void setCarbonMonoxide(double carbonMonoxide) {
        this.carbonMonoxide = carbonMonoxide;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getStationName() {
        return stationName;
    }

    public void setStationName(String stationName) {
        this.stationName = stationName;
    }

    // recommended in flink docs to override
    @Override
    public String toString() {
        return "station name: " + this.stationName + ", carbon monoxide: " + this.carbonMonoxide;
    }
}