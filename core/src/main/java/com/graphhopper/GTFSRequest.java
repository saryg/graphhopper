package com.graphhopper;

import java.sql.Timestamp;
import java.util.Calendar;

import com.graphhopper.util.shapes.GHPoint;
/**
 * TODO: Document
 * <code>GTFSRequest</code>
 *
 * @author Seán Healy
 *
 */
public class GTFSRequest {
	private static final String[] DAYS = new String[]{
		null, "sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"
	};
	private GHPoint from;
	private GHPoint to;
	private Calendar calendar;
	public GTFSRequest (GHPoint from, GHPoint to, String time) {
		this.from = from;
		this.to = to;
		calendar = Calendar.getInstance();
		calendar.setTimeInMillis(Timestamp.valueOf(time).getTime());
	}
	public double getFromLat() {
		return from.lat;
	}
	public GHPoint getFromPoint() {
		return from;
	}
	public double fromLon() {
		return from.lon;
	}
	public String day() {
		return DAYS[calendar.get(Calendar.DAY_OF_WEEK)];
	}
	public int time() {
		return calendar.get(Calendar.SECOND) +
			calendar.get(Calendar.MINUTE) * 60 +
			calendar.get(Calendar.HOUR) * 3600;
	}
}
