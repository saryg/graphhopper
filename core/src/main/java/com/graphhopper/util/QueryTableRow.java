package com.graphhopper.util;

import java.util.Hashtable;
/**
 * TODO: Document
 * <code>QueryTableRow</code>
 *
 * @author Seán Healy
 *
 */
@SuppressWarnings("rawtypes")
public class QueryTableRow extends Hashtable<String, Comparable> {
	public QueryTableRow(int i) {
		super(i);
	}

	private static final long serialVersionUID = 6822347611722508705L;
}
