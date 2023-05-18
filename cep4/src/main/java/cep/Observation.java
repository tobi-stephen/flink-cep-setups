package cep;

public class Observation {

    String id;
    int carbonMonoxide;
    long timeInterval;

    public Observation(String id, int carbonMonoxide, long timeInterval) {
        this.id = id;
        this.carbonMonoxide = carbonMonoxide;
        this.timeInterval = timeInterval;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    @Override
    public String toString() {
        return "Observation{" +
                "id =" + id +
                ", carbonmonoxide='" + carbonMonoxide + '\'' +
                ", time interval=" + timeInterval +
                '}';
    }
}
