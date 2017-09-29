package lodi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TreeMap;

/**
 * A wrapper for the google_cities table in the geolocation database.
 */
public class GoogleCities {

    /**
     * A struct representing a record from the google_cities table
     */
    public static class Record {
        public final String inputString;
        public final double confidence;
        public final Cities.Record city;

        public Record(String inputString, double confidence, Cities.Record city)
        {
            this.inputString = inputString;
            this.confidence = confidence;
            this.city = city;
        }
    }

    /**
     * Construct an in-memory copy of the google_cities table.
     *
     * @param conn Connection to the geolocation database
     * @param confidenceThreshold Only records with a confidence value greater
     * than this threshold will be loaded.
     * @param cities A copy of the cities table. This is used to set up links from
     * the google_cities table.
     */
    public GoogleCities(Connection conn, double confidenceThreshold, Cities cities)
        throws SQLException
    {
        map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        
        String sql = 
            "select input_string, city_id, confidence " +
            "from google_cities " +
            "join cities on cities.id = city_id " +
            "where confidence > ? " +
            "and (city <> '' or region <> '')";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDouble(1, confidenceThreshold);
            ResultSet rs = pstmt.executeQuery();
                     
            while (rs.next()) {
                String inputString = rs.getString(1);
                int cityId = rs.getInt(2);
                double confidence = rs.getDouble(3);

                Cities.Record city = cities.get(cityId);

                Record rec = new Record(inputString, confidence, city);
                map.put(rec.inputString, rec);
            }
        }
    }

    /**
     * Tests whether the given cleanedLocation appears as a known input location
     * in the google cities table.
     *
     * @param cleanedLocation The cleaned input location to test
     */
    public boolean containsKey(String cleanedLocation) {
        return map.containsKey(cleanedLocation);
    }

    /**
     * Returns the record matching the given input location.
     *
     * @param cleanedLocation The cleaned input location
     * @return The google cities record matching the input location
     */
    public Record get(String cleanedLocation) {
        return map.get(cleanedLocation);
    }

    /**
     * Returns the number of records in the table.
     */
    public int size() {
        return map.size();
    }

    private final TreeMap<String, Record> map;
}
