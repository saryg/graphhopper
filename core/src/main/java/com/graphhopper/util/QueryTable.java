package com.graphhopper.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

public class QueryTable implements Iterable<QueryTableRow> {
	public class QueryTableIterator implements Iterator<QueryTableRow> {
		Iterator<QueryTableRow> current = new Stack<QueryTableRow>().iterator();
		@Override
		public boolean hasNext() {
			if (current.hasNext()) {
				return true;
			}
			while (iterator.hasNext()) {
				current = iterator.next().getValue().iterator();
				if (current.hasNext()) {
					return true;
				}
			}
			return false;
		}

		@Override
		public QueryTableRow next() {
			return current.next();
		}

		@Override
		public void remove() {
		}

	}
	private static final String BOOL = "[01]";
	private static final String TIME = "\\d\\d:[0-5]\\d:[0-5]\\d";
	private static final String NUMBER = "-?(([1-9]\\d*)|0)";
	private enum Type {
		EMPTY, BOOL, BYTE, SHORT, TIME_IN_SECONDS, INT, LONG, BIGINT, DOUBLE, BIGDECIMAL, STR;

		@SuppressWarnings("rawtypes")
		public Comparable parseType(String str) {
			switch (this) {
				case BOOL:
					return str.charAt(0) == '1' ? true : false;
				case BYTE:
					return Byte.parseByte(str);
				case SHORT:
					return Short.parseShort(str);
				case INT:
					return Integer.parseInt(str);
				case TIME_IN_SECONDS:
					return Integer.parseInt(str.substring(0,2)) * 3600
						+ Integer.parseInt(str.substring(3,5)) * 60
						+ Integer.parseInt(str.substring(6,8));
				case LONG:
					return Long.parseLong(str);
				case BIGINT:
					return new BigInteger(str);
				case DOUBLE:
					return Double.parseDouble(str);
				case BIGDECIMAL:
					return new BigDecimal(str);
				default:
					return str;
			}
		}
	}
	private Hashtable<String, Type> types = new Hashtable<String, Type>(0);
	@SuppressWarnings("rawtypes")
	private Hashtable<String, TreeMap<Comparable, Stack<QueryTableRow>>> table =
		new Hashtable<String, TreeMap<Comparable, Stack<QueryTableRow>>>(0);
	private String orderBy = null;
	@SuppressWarnings("rawtypes")
	private Iterator<Entry<Comparable, Stack<QueryTableRow>>> iterator;
	@SuppressWarnings("rawtypes")
	public QueryTable(Object[][] csv) {
		Object[] columns = csv[0];
		int height = csv.length;
		int width = columns.length;
		detectColumnTypes(csv);
		for (Object column : columns) {
			table.put(column.toString(), new TreeMap<Comparable, Stack<QueryTableRow>>());
		}
		QueryTableRow[] objCSV = new QueryTableRow[height];
		for (int i = 1; i < height; i++) {
			objCSV[i] = new QueryTableRow(0);
			for (int j = 0; j < width; j++) {
				String cell = csv[i][j].toString();
				Type type = types.get(columns[j]);
				Comparable obj = type.parseType(cell);
				objCSV[i].put(columns[j].toString(), obj);
			}
		}
		for (int i = 1; i < height; i++) {
			for (int j = 0; j < width; j++) {
				TreeMap<Comparable, Stack<QueryTableRow>> column = table.get(columns[j]);
				if (!column.containsKey(objCSV[i].get(columns[j]))) {
					column.put(objCSV[i].get(columns[j]), new Stack<QueryTableRow>());
				}
				column.get(objCSV[i].get(columns[j])).push(objCSV[i]);
			}
		}
	}
	@SuppressWarnings("rawtypes")
	public QueryTable(Stack<QueryTableRow> rows, String orderBy) {
		this.orderBy = orderBy;
		int height = rows.size();
		if (height == 0) {
			return;
		}
		Set<String> columns = rows.peek().keySet();
		for (String column : columns) {
			table.put(column, new TreeMap<Comparable, Stack<QueryTableRow>>());
		}
		QueryTableRow[] objCSV = new QueryTableRow[height];
		for (int i = 0; i < height; i++) {
			objCSV[i] = new QueryTableRow(0);
			for (String column : columns) {
				Comparable obj = rows.get(i).get(column);
				objCSV[i].put(column, obj);
			}
		}
		for (int i = 0; i < height; i++) {
			for (String columnKey : columns) {
				TreeMap<Comparable, Stack<QueryTableRow>> column = table.get(columnKey);
				if (!column.containsKey(objCSV[i].get(columnKey))) {
					column.put(objCSV[i].get(columnKey), new Stack<QueryTableRow>());
				}
				column.get(objCSV[i].get(columnKey)).push(objCSV[i]);
			}
		}
	}
	private void detectColumnTypes (Object[][] csv) {
		for (int j = 0; j < csv[0].length; j++) {
			byte type = 0;
			for (int i = 1; i < csv.length; i++) {
				String cell = csv[i][j].toString();
				if (type == 0 && cell.isEmpty()) {
					
				}
				else if (type <= 1 && cell.matches(BOOL)) {
					type = 1;
				}
				else if (type <= 2 && isByte(cell)) {
					type = 2;
				}
				else if (type <= 3 && isShort(cell)) {
					type = 3;
				}
				else if (type <= 4 && isTimeInSeconds(cell)) {
					type = 4;
				}
				else if (type <= 5 && isInt(cell)) {
					type = 5;
				}
				else if (type <= 6 && isLong(cell)) {
					type = 6;
				}
				else if (type <= 7 && isBigInt(cell)) {
					type = 7;
				}
				else if (type <= 8 && isDouble(cell)) {
					type = 8;
				}
				else if (type <= 9 && isBigDouble(cell)) {
					type = 9;
				}
				else {
					type = 10;
					break;
				}
			}
			types.put(csv[0][j].toString(), Type.values()[type]);
		}
	}
	private static boolean isTimeInSeconds(String cell) {
		return cell.length() == 8 && cell.matches(TIME);
	}
	private static boolean isBigDouble(String cell) {
		try {
			new BigDecimal(cell);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}
	private static boolean isDouble(String cell) {
		try {
			Double.parseDouble(cell);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}
	private static boolean isBigInt(String cell) {
		return cell.matches(NUMBER);
	}
	private static boolean isLong(String cell) {
		if (cell.length() > 20 || !cell.matches(NUMBER)) {
			return false;
		}
		BigInteger rep = new BigInteger(cell);
		return rep.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) >= 0 &&
			rep.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0;
	}
	private static boolean isInt(String cell) {
		if (cell.length() > 11 || !cell.matches(NUMBER)) {
			return false;
		}
		long rep = Long.parseLong(cell);
		return rep >= Integer.MIN_VALUE && rep <= Integer.MAX_VALUE;
	}
	private static boolean isShort(String cell) {
		if (cell.length() > 6 || !cell.matches(NUMBER)) {
			return false;
		}
		int rep = Integer.parseInt(cell);
		return rep >= Short.MIN_VALUE && rep <= Short.MAX_VALUE;
	}
	private static boolean isByte(String cell) {
		if (cell.length() > 4 || !cell.matches(NUMBER)) {
			return false;
		}
		short rep = Short.parseShort(cell);
		return rep >= Byte.MIN_VALUE && rep <= Byte.MAX_VALUE;
	}
	public QueryTable get(String column, Object value) {
		return new QueryTable(get(column).get(value), column);
	}
	@SuppressWarnings("rawtypes")
	public TreeMap<Comparable, Stack<QueryTableRow>> get(String column) {
		return table.get(column);
	}
	@SuppressWarnings("rawtypes")
	public Stack<Comparable> select(String selectColumn, String whereThis, Comparable equalsThis) {
		QueryTable results = get(whereThis, equalsThis);
		Stack<Comparable> output = new Stack<Comparable>();
		for (QueryTableRow result : results) {
			output.push(result.get(selectColumn));
		}
		return output;
	}
	@Override
	public QueryTableIterator iterator() {
		if (orderBy == null) {
			orderBy = table.keys().nextElement();
		}
		this.iterator = table.get(orderBy).entrySet().iterator();
		return new QueryTableIterator();
	}
	public void orderBy(String orderBy) {
		this.orderBy = orderBy;
	}
}
