package cep;

public class Pollution {

    double carbonMonoxide;
    long timeInterval;

    public Pollution(double carbonMonoxide, long timeInterval) {
        this.carbonMonoxide = carbonMonoxide;
        this.timeInterval = timeInterval;
    }

    public double getCarbonMonoxide() {
        return carbonMonoxide;
    }

    public void setCarbonMonoxide(double carbonMonoxide) {
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
                ", carbonmonoxide='" + carbonMonoxide + '\'' +
                ", time interval=" + timeInterval +
                '}';
    }
}
