package lodi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * A wrapper for the cities table in the geolocation database.
 */
public class Cities {

    /**
     * A struct representing a record in the cities table.
     */
    public static class Record {
        public final int id;
        public final String city;
        public final String region;
        public final String country;
        public final double latitude;
        public final double longitude;
        public final String stringValue;

        public Record(int id, String city, String region, String country,
                      double latitude, double longitude, String stringValue) 
        {
            this.id = id;
            this.city = city;
            this.region = region;
            this.country = country;
            this.latitude = latitude;
            this.longitude = longitude;
            this.stringValue = stringValue;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }

    /**
     * Construct an in-memory copy of the cities table.
     *
     * @param conn Connection to the geolocation database
     */
    public Cities(Connection conn) 
        throws SQLException
    {
        countryMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        stateMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        countryCityMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        stateCityMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        idMap = new TreeMap<>();

        try (Statement stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery(
                    "select id, city, region, country, latitude, longitude , string_value " +
                    "from cities " +
                    "where city <> '' or region <> ''");

            while (rs.next()) {
                int id = rs.getInt(1);
                String city = rs.getString(2);
                String region = rs.getString(3);
                String country = rs.getString(4);
                double latitude = rs.getDouble(5);
                double longitude = rs.getDouble(6);
                String stringValue = rs.getString(7);

                Record rec = new Record(id, city, region, country, latitude, longitude, stringValue);

                // This is to prevent null-pointer exceptions when using these values as hash keys

                if (city == null) city = "";
                if (region == null) region = "";
                if (country == null) country = "";

                idMap.put(id, rec);

                if (countryMap.containsKey(country)) {
                    countryMap.get(country).add(rec);
                    countryCityMap.get(country).put(city, rec);
                }
                else {
                    LinkedList<Record> list = new LinkedList<>();
                    list.add(rec);
                    countryMap.put(country, list);

                    TreeMap<String, Record> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    map.put(city, rec);
                    countryCityMap.put(country, map);
                }

                if ("US".equalsIgnoreCase(country)) {
                    if (stateMap.containsKey(region)) {
                        stateMap.get(region).add(rec);
                        stateCityMap.get(region).put(city, rec);
                    }
                    else {
                        LinkedList<Record> list = new LinkedList<>();
                        list.add(rec);
                        stateMap.put(region, list);

                        TreeMap<String, Record> map = new TreeMap<>();
                        map.put(city, rec);
                        stateCityMap.put(region, map);
                    }
                }
            }
        }
    }
    
    /**
     * Get the city record with the given ID.
     */
    public Record get(int id) {
        return idMap.get(id);
    }

    public Record getCityInCountry(String city, String country) {
        if (city == null || country == null || !countryCityMap.containsKey(country))
            return null;

        return countryCityMap.get(country).get(city);
    }

    public Record getCityInState(String city, String state) {

        if (city == null || state == null || !stateCityMap.containsKey(state))
            return null;

        return stateCityMap.get(state).get(city);
    }

    /**
     * Get a list of all cities in the given country. If the country is unknown,
     * then the return value is null.
     *
     * @param country The country code to search for
     */
    public List<Record> getCountry(String country) {
        return countryMap.get(country);
    }
    
    /**
     * Get a list of all cities in the given state. If the state is unknown,
     * then the return value is null.
     *
     * @param state The state code to search for
     */
    public List<Record> getState(String state) {
        return stateMap.get(state);
    }

    /**
     * Returns the number of records in the table.
     */
    public int size() {
        return idMap.size();
    }

    private final TreeMap<String, LinkedList<Record>> countryMap;
    private final TreeMap<String, LinkedList<Record>> stateMap;
    private final TreeMap<String, TreeMap<String, Record>> countryCityMap;
    private final TreeMap<String, TreeMap<String, Record>> stateCityMap;
    private final TreeMap<Integer, Record> idMap;
}
