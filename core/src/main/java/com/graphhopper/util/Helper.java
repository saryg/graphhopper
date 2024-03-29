/*
 *  Licensed to GraphHopper and Peter Karich under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for
 *  additional information regarding copyright ownership.
 *
 *  GraphHopper licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except in
 *  compliance with the License. You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.graphhopper.util;

import com.graphhopper.util.shapes.BBox;
import com.graphhopper.util.shapes.GHPoint;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Several utility classes which are compatible with Java6 on Android.
 * <p>
 * @author Peter Karich
 * @see Helper7 for none-Android compatible methods.
 */
public class Helper
{

	private static final long EARTH_DIAMETER = 12746;
    public static final DistanceCalc DIST_EARTH = new DistanceCalcEarth();
    public static final DistanceCalc3D DIST_3D = new DistanceCalc3D();
    public static final DistancePlaneProjection DIST_PLANE = new DistancePlaneProjection();
    private static final Logger logger = LoggerFactory.getLogger(Helper.class);
    public static Charset UTF_CS = Charset.forName("UTF-8");
    public static final long MB = 1L << 20;

    public static ArrayList<Integer> tIntListToArrayList( TIntList from )
    {
        int len = from.size();
        ArrayList<Integer> list = new ArrayList<Integer>(len);
        for (int i = 0; i < len; i++)
        {
            list.add(from.get(i));
        }
        return list;
    }

    public static Locale getLocale( String param )
    {
        int pointIndex = param.indexOf('.');
        if (pointIndex > 0)
            param = param.substring(0, pointIndex);

        param = param.replace("-", "_");
        int index = param.indexOf("_");
        if (index < 0)
        {
            return new Locale(param);
        }
        return new Locale(param.substring(0, index), param.substring(index + 1));
    }

    static String packageToPath( Package pkg )
    {
        return pkg.getName().replaceAll("\\.", File.separator);
    }

    public static int countBitValue( int maxTurnCosts )
    {
        double val = Math.log(maxTurnCosts) / Math.log(2);
        int intVal = (int) val;
        if (val == intVal)
            return intVal;
        return intVal + 1;
    }

    private Helper()
    {
    }

    public static void loadProperties( Map<String, String> map, Reader tmpReader ) throws IOException
    {
        BufferedReader reader = new BufferedReader(tmpReader);
        String line;
        try
        {
            while ((line = reader.readLine()) != null)
            {
                if (line.startsWith("//") || line.startsWith("#"))
                {
                    continue;
                }

                if (Helper.isEmpty(line))
                {
                    continue;
                }

                int index = line.indexOf("=");
                if (index < 0)
                {
                    logger.warn("Skipping configuration at line:" + line);
                    continue;
                }

                String field = line.substring(0, index);
                String value = line.substring(index + 1);
                map.put(field.trim(), value.trim());
            }
        } finally
        {
            reader.close();
        }
    }

    public static void saveProperties( Map<String, String> map, Writer tmpWriter ) throws IOException
    {
        BufferedWriter writer = new BufferedWriter(tmpWriter);
        try
        {
            for (Entry<String, String> e : map.entrySet())
            {
                writer.append(e.getKey());
                writer.append('=');
                writer.append(e.getValue());
                writer.append('\n');
            }
        } finally
        {
            writer.close();
        }
    }

    public static List<String> readFile( String file ) throws IOException
    {
        return readFile(new InputStreamReader(new FileInputStream(file), UTF_CS));
    }

    public static List<String> readFile( Reader simpleReader ) throws IOException
    {
        BufferedReader reader = new BufferedReader(simpleReader);
        try
        {
            List<String> res = new ArrayList<String>();
            String line;
            while ((line = reader.readLine()) != null)
            {
                res.add(line);
            }
            return res;
        } finally
        {
            reader.close();
        }
    }

    public static String isToString( InputStream inputStream ) throws IOException
    {
        int size = 1024 * 8;
        String encoding = "UTF-8";
        InputStream in = new BufferedInputStream(inputStream, size);
        try
        {
            byte[] buffer = new byte[size];
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            int numRead;
            while ((numRead = in.read(buffer)) != -1)
            {
                output.write(buffer, 0, numRead);
            }
            return output.toString(encoding);
        } finally
        {
            in.close();
        }
    }

    public static int idealIntArraySize( int need )
    {
        return idealByteArraySize(need * 4) / 4;
    }

    public static int idealByteArraySize( int need )
    {
        for (int i = 4; i < 32; i++)
        {
            if (need <= (1 << i) - 12)
            {
                return (1 << i) - 12;
            }
        }
        return need;
    }

    public static boolean removeDir( File file )
    {
        if (!file.exists())
        {
            return true;
        }

        if (file.isDirectory())
        {
            for (File f : file.listFiles())
            {
                removeDir(f);
            }
        }

        return file.delete();
    }

    public static long getTotalMB()
    {
        return Runtime.getRuntime().totalMemory() / MB;
    }

    public static long getUsedMB()
    {
        return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / MB;
    }

    public static String getMemInfo()
    {
        return "totalMB:" + getTotalMB() + ", usedMB:" + getUsedMB();
    }

    public static int getSizeOfObjectRef( int factor )
    {
        // pointer to class, flags, lock
        return factor * (4 + 4 + 4);
    }

    public static int getSizeOfLongArray( int length, int factor )
    {
        // pointer to class, flags, lock, size
        return factor * (4 + 4 + 4 + 4) + 8 * length;
    }

    public static int getSizeOfObjectArray( int length, int factor )
    {
        // improvements: add 4byte to make a multiple of 8 in some cases plus compressed oop
        return factor * (4 + 4 + 4 + 4) + 4 * length;
    }

    public static void close( Closeable cl )
    {
        try
        {
            if (cl != null)
                cl.close();
        } catch (IOException ex)
        {
            throw new RuntimeException("Couldn't close resource", ex);
        }
    }

    public static boolean isEmpty( String str )
    {
        return str == null || str.trim().length() == 0;
    }
    /**
     * Determines if the specified ByteBuffer is one which maps to a file!
     */
    public static boolean isFileMapped( ByteBuffer bb )
    {
        if (bb instanceof MappedByteBuffer)
        {
            try
            {
                ((MappedByteBuffer) bb).isLoaded();
                return true;
            } catch (UnsupportedOperationException ex)
            {
            }
        }
        return false;
    }

    public static int calcIndexSize( BBox graphBounds )
    {
        if (!graphBounds.isValid())
            throw new IllegalArgumentException("Bounding box is not valid to calculate index size: " + graphBounds);

        double dist = DIST_EARTH.calcDist(graphBounds.maxLat, graphBounds.minLon,
                graphBounds.minLat, graphBounds.maxLon);
        // convert to km and maximum is 50000km => 1GB
        dist = Math.min(dist / 1000, 50000);
        return Math.max(2000, (int) (dist * dist));
    }

	public static double distance (GHPoint a, GHPoint b) {
	    double lat = Math.toRadians(b.lat - a.lat) / 2;
	    double lon = Math.toRadians(b.lon - a.lon) / 2;
	    double c = Math.sin(lat) * Math.sin(lat) +
	        Math.cos(Math.toRadians(a.lat)) *
	        Math.cos(Math.toRadians(b.lat)) *
	        Math.sin(lon) * Math.sin(lon);
	    return EARTH_DIAMETER * Math.atan2(Math.sqrt(c), Math.sqrt(1 - c));
	}

    public static String pruneFileEnd( String file )
    {
        int index = file.lastIndexOf(".");
        if (index < 0)
            return file;
        return file.substring(0, index);
    }

    public static TIntList createTList( int... list )
    {
        TIntList res = new TIntArrayList(list.length);
        for (int val : list)
        {
            res.add(val);
        }
        return res;
    }

    public static PointList createPointList( double... list )
    {
        if (list.length % 2 != 0)
            throw new IllegalArgumentException("list should consist of lat,lon pairs!");

        int max = list.length / 2;
        PointList res = new PointList(max, false);
        for (int i = 0; i < max; i++)
        {
            res.add(list[2 * i], list[2 * i + 1], Double.NaN);
        }
        return res;
    }

    public static PointList createPointList3D( double... list )
    {
        if (list.length % 3 != 0)
            throw new IllegalArgumentException("list should consist of lat,lon,ele tuples!");

        int max = list.length / 3;
        PointList res = new PointList(max, true);
        for (int i = 0; i < max; i++)
        {
            res.add(list[3 * i], list[3 * i + 1], list[3 * i + 2]);
        }
        return res;
    }

    /**
     * Converts into an integer to be compatible with the still limited DataAccess class (accepts
     * only integer values). But this conversion also reduces memory consumption where the precision
     * loss is accceptable. As +- 180° and +-90° are assumed as maximum values.
     * <p>
     * @return the integer of the specified degree
     */
    public static final int degreeToInt( double deg )
    {
        if (deg >= Double.MAX_VALUE)
            return Integer.MAX_VALUE;
        if (deg <= -Double.MAX_VALUE)
            return -Integer.MAX_VALUE;
        return (int) (deg * DEGREE_FACTOR);
    }

    /**
     * Converts back the integer value.
     * <p>
     * @return the degree value of the specified integer
     */
    public static final double intToDegree( int storedInt )
    {
        if (storedInt == Integer.MAX_VALUE)
            return Double.MAX_VALUE;
        if (storedInt == -Integer.MAX_VALUE)
            return -Double.MAX_VALUE;
        return (double) storedInt / DEGREE_FACTOR;
    }

    /**
     * Converts elevation value (in meters) into integer for storage.
     */
    public static final int eleToInt( double ele )
    {
        if (ele >= Integer.MAX_VALUE)
            return Integer.MAX_VALUE;
        return (int) (ele * ELE_FACTOR);
    }

    /**
     * Converts the integer value retrieved from storage into elevation (in meters). Do not expect
     * more precision than meters although it currently is!
     */
    public static final double intToEle( int integEle )
    {
        if (integEle == Integer.MAX_VALUE)
            return Double.MAX_VALUE;
        return integEle / ELE_FACTOR;
    }

    // +- 180 and +-90 => let use use 400
    private static final float DEGREE_FACTOR = Integer.MAX_VALUE / 400f;
    // milli meter is a bit extreme but we have integers
    private static final float ELE_FACTOR = 1000f;

    public static void cleanMappedByteBuffer( final ByteBuffer buffer )
    {
        try
        {
            AccessController.doPrivileged(new PrivilegedExceptionAction<Object>()
            {
                @Override
                public Object run() throws Exception
                {
                    try
                    {
                        final Method getCleanerMethod = buffer.getClass().getMethod("cleaner");
                        getCleanerMethod.setAccessible(true);
                        final Object cleaner = getCleanerMethod.invoke(buffer);
                        if (cleaner != null)
                            cleaner.getClass().getMethod("clean").invoke(cleaner);
                    } catch (NoSuchMethodException ex)
                    {
                        // ignore if method cleaner or clean is not available, like on Android
                    }
                    return null;
                }
            });
        } catch (PrivilegedActionException e)
        {
            throw new RuntimeException("unable to unmap the mapped buffer", e);
        }
    }

    /**
     * Trying to force the release of the mapped ByteBuffer. See
     * http://stackoverflow.com/q/2972986/194609 and use only if you know what you are doing.
     */
    public static void cleanHack()
    {
        System.gc();
    }

    public static String nf( long no )
    {
        // I like french localization the most: 123654 will be 123 654 instead
        // of comma vs. point confusion for english/german guys.
        // NumberFormat is not thread safe => but getInstance looks like it's cached
        return NumberFormat.getInstance(Locale.FRANCE).format(no);
    }

    public static String firstBig( String sayText )
    {
        if (sayText == null || sayText.length() <= 0)
        {
            return sayText;
        }

        return Character.toUpperCase(sayText.charAt(0)) + sayText.substring(1);
    }

    /**
     * This methods returns the value or min if too small or max if too big.
     */
    public static final double keepIn( double value, double min, double max )
    {
        return Math.max(min, Math.min(value, max));
    }

    /**
     * Round the value to the specified exponent
     */
    public static double round( double value, int exponent )
    {
        double factor = Math.pow(10, exponent);
        return Math.round(value * factor) / factor;
    }

    public static final double round6( double value )
    {
        return Math.round(value * 1e6) / 1e6;
    }

    public static final double round4( double value )
    {
        return Math.round(value * 1e4) / 1e4;
    }

    public static final double round2( double value )
    {
        return Math.round(value * 100) / 100d;
    }

    /**
     * This creates a date formatter for yyyy-MM-dd'T'HH:mm:ss'Z' which is has to be identical to
     * buildDate used in pom.xml
     */
    public static DateFormat createFormatter()
    {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df;
    }

	public static <T> HashSet<T> fillSet(T[] objects) {
		HashSet<T> set = new HashSet<T>(0);
		for (T object : objects) {
			set.add(object);
		}
		return set;
	}
}
