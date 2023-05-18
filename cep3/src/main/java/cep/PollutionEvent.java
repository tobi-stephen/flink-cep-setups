package cep;

public class PollutionEvent {

	private String station;
	private int carbon_monoxide;

	public PollutionEvent(String station, int carbon_monoxide) {
		this.station = station;
		this.carbon_monoxide = carbon_monoxide;
	}

	public String getStation() {
		return station;
	}

	public void setStation(String station) {
		this.station = station;
	}

	public int getCarbon_monoxide() {
		return carbon_monoxide;
	}

	public void setCarbon_monoxide(int carbon_monoxide) {
		this.carbon_monoxide = carbon_monoxide;
	}
}