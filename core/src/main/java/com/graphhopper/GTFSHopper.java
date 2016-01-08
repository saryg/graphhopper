package com.graphhopper;

import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.core.ZipFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.TreeMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.openstreetmap.osmosis.osmbinary.file.FileFormatException;

import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.util.Helper;
import com.graphhopper.util.QueryTable;
import com.graphhopper.util.QueryTableRow;
import com.graphhopper.util.shapes.GHPoint;
/**
 * TODO: Document
 * <code>GTFSReader</code>
 *
 * @author Seán Healy
 *
 */
public class GTFSHopper extends GraphHopper {
	private static final double WALKING_RADIUS = 1.5;
	private static final long WALKING_TIME = 15 * 60 * 1000;
	public static void main (String[] args) throws IOException {
		GTFSHopper hopper = new GTFSHopper().
			setGTFSFile("gtfs.zip").
			setGTFSHopperLocation("gtfs");
		hopper.setCHEnable(false);
        hopper.setEncodingManager(new EncodingManager(EncodingManager.FOOT));
		hopper.setOSMFile("map.osm");
		hopper.setGraphHopperLocation("tmp");
		hopper.importOrLoad();
		hopper.gtfsRoute(
			new GTFSRequest(
				new GHPoint(53.422832, -6.133461),
				new GHPoint(53.352191, -6.26461),
				"2016-01-09 08:00:00"
			)
		);
	}
	private static final HashSet<String> GTFS_FILES =
		Helper.fillSet(
			new String[]{
				"agency.txt",
				"calendar.txt",
				"calendar_dates.txt",
				"fare_attributes.txt",
				"fare_rules.txt",
				"feed_info.txt",
				"frequencies.txt",
				"routes.txt",
				"shapes.txt",
				"stops.txt",
				"stop_times.txt",
				"transfers.txt",
				"trips.txt"
			}
		);
	private static final int MAX_WAIT_TIME = 15 * 60;
	Hashtable<String, QueryTable> files = new Hashtable<String, QueryTable>(0);
	private byte filesLoaded = 0;
	private String gtfshLocation = null;
	private String gtfsFile = null;
	public GTFSHopper setGTFSFile( String gtfsFile ) {
		this.gtfsFile = gtfsFile;
		return this;
	}
	public GTFSHopper setGTFSHopperLocation( String gtfshLocation ) {
		this.gtfshLocation = gtfshLocation;
		return this;
	}
	@SuppressWarnings("rawtypes")
	public GTFSResponse gtfsRoute( GTFSRequest request ) {
		QueryTable stopsTable = files.get("stops.txt");
		QueryTable stopTimesTable = files.get("stop_times.txt");
		QueryTable calendarTable = files.get("calendar.txt");
		QueryTable tripsTable = files.get("trips.txt");
		QueryTable frequenciesTable = files.get("frequencies.txt");
		String day = request.day();
		Stack<Comparable> services = calendarTable.select("service_id", day, true);
		HashSet<Comparable> trips = new HashSet<Comparable>(0);
		for (Comparable service : services) {
			trips.addAll(
				tripsTable.select("trip_id", "service_id", service)
			);
		}
		TreeMap<Long, Object> walkingRadiusStops =
			getWalkingRadiusStops(request.getFromPoint(), stopsTable);
		
		for (Entry<Long, Object> stopId : walkingRadiusStops.entrySet()) {
			int arrival = (int) (request.time() + stopId.getKey() / 1000);
			QueryTable stops = stopTimesTable.get("stop_id", stopId.getValue());
			Stack<QueryTableRow> stopTimes = new Stack<QueryTableRow>();
			for (QueryTableRow stop : stops) {
				Object tripId = stop.get("trip_id");
				if (trips.contains(tripId)) {
					stopTimes.push(stop);
					QueryTable frequencies = frequenciesTable.get("trip_id", tripId);
					frequencies.orderBy("start_time");
					for (QueryTableRow row : frequencies) {
						int arrivalTime = (int) stop.get("arrival_time");
						int departureTime = (int) stop.get("departure_time");
						int startTime = (int) row.get("start_time");
						int maxWait = arrival + MAX_WAIT_TIME;
						if (startTime + arrivalTime > maxWait) {
							break;
						}
						int endTime = (int) row.get("end_time") + arrivalTime;
						if (endTime < arrival) {
							continue;
						}
						int headway = (int) row.get("headway_secs");
						for (int time = startTime; time + arrivalTime <= endTime; time += headway) {
							if (time + arrivalTime > arrival) {
								arrivalTime = time + arrivalTime;
								departureTime = time + departureTime;
								System.out.println(
									tripId + " " + arrivalTime + " " + departureTime + " " + stopId.getValue()
								);
								break;
							}
						}
					}
				}
			}
		}
		return null;
	}
	@SuppressWarnings("rawtypes")
	private TreeMap<Long, Object> getWalkingRadiusStops( GHPoint point, QueryTable stopsTable ) {
		TreeMap<Comparable, Stack<QueryTableRow>> orderedByLat =
			stopsTable.get("stop_lat");
		Object[] lats = orderedByLat.keySet().toArray();
		int nearestLat = Math.abs(
			Arrays.binarySearch(lats, point.lat)
		);
		TreeMap<Long, Object> walkingRadiusStops = new TreeMap<Long, Object>();
		// Stops above the starting point
		int i = nearestLat;
		while (i >= 0 && sieveLocalStops(point, orderedByLat, lats[i--], walkingRadiusStops));
		// Stops below the starting point
		i = nearestLat + 1;
		while (i < lats.length && sieveLocalStops(point, orderedByLat, lats[i++], walkingRadiusStops));
		return walkingRadiusStops;
	}
	@SuppressWarnings("rawtypes")
	private boolean sieveLocalStops(
		GHPoint point,
		TreeMap<Comparable, Stack<QueryTableRow>> orderedByLat,
		Object lat,
		TreeMap<Long, Object> walkingRadiusStops
	) {
		if (
			Helper.distance(
				point,
				new GHPoint((double) lat, point.lon)
			) > WALKING_RADIUS
		) {
			return false;
		}
		Stack<QueryTableRow> stops = orderedByLat.get(lat);
		for (QueryTableRow stop : stops) {
			lat = stop.get("stop_lat");
			double lon = (double) stop.get("stop_lon");
			GHPoint stopPoint = new GHPoint((double) lat, lon);
			double distance = Helper.distance(point, stopPoint);
			if (distance < WALKING_RADIUS) {
				GHResponse resp = route(new GHRequest(point, stopPoint));
				if (!resp.hasErrors()) {
					long time = resp.getTime();
					if (time < WALKING_TIME) {
						walkingRadiusStops.put(time, stop.get("stop_id"));
					}
				}
			}
		}
		return true;
	}
	public GTFSHopper importOrLoad() {
		super.importOrLoad();
		File gtfsDir = new File(gtfshLocation);
		if (!gtfsDir.exists()) {
		    try {
		         ZipFile zipFile = new ZipFile(gtfsFile);
		         zipFile.extractAll(gtfshLocation);
		    } catch (ZipException e) {
		        e.printStackTrace();
		    }
		}
		for (File gtfsFile : gtfsDir.listFiles()) {
			String[][] csv = null;
			try {
				csv = readCSVFile(gtfsFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
			QueryTable table = new QueryTable(csv);
			String name = gtfsFile.getName();
			if (!GTFS_FILES.contains(name)) {
				try {
					throw new FileFormatException(
						"The file '" + name + "' is not a GTFS file."
					);
				} catch (FileFormatException e) {
					e.printStackTrace();
				}
			}
			else {
				filesLoaded++;
			}
			files.put(name, table);
		}
		if (filesLoaded != 13) {
			try {
				throw new ZipException("The GTFS zip folder should contain 13 files.");
			} catch (ZipException e) {
				e.printStackTrace();
			}
		}
		return this;
	}
	private static String[][] readCSVFile( File file ) throws IOException {
		BufferedReader reader = new BufferedReader(
			new FileReader(
				file
			)
		);
		Iterator<CSVRecord> records = CSVFormat.DEFAULT.parse(reader).iterator();
		CSVRecord record = records.next();
		int length = record.size();
		String[] columns = new String[length];
		for (int i = 0; i < length; i++) {
			columns[i] = record.get(i);
		}
		Stack<String[]> table = new Stack<String[]>();
		table.push(columns);
		while (records.hasNext()) {
			record = records.next();
			String[] row = new String[length];
			for (int i = 0; i < length; i++) {
				row[i] = record.get(i);
			}
			table.push(row);
		}
		reader.close();
		length = table.size();
		String[][] output = new String[length][];
		for (int i = length - 1; i >= 0; i--) {
			output[i] = table.pop();
		}
		return output;
	}
}
