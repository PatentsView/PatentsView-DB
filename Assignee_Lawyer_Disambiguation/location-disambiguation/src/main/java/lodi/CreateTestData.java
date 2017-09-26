package lodi;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CreateTestData {
    public static void main(String[] args) 
        throws ClassNotFoundException, IOException, SQLException
    { 	
        // Read the list of location IDs

        LinkedList<String> locationIds = new LinkedList<>();
        
        try (Stream<String> lines = 
                Files.lines(Paths.get("coded_locations_old.csv"), Charset.forName("ISO-8859-1"))) 
        {
            lines.skip(1).forEach(line -> {
                String[] fields = line.split(",");
                int n = fields[0].length();
                String id = fields[0].substring(1, n - 1);
                locationIds.add(id);
            });
        }

        // locationIds.stream().limit(10).forEach(id -> System.out.println(id));

        // Read the configuration
        
        App.Configuration config = App.loadConfiguration("config.properties");
        Class.forName("com.mysql.jdbc.Driver");
        Class.forName("org.sqlite.JDBC");

        Connection conn = DriverManager.getConnection(config.locationDatabase);
        conn.setAutoCommit(false);

        DriverManager.setLoginTimeout(10);
        Connection pdb = DriverManager.getConnection(
                config.patentDatabase, config.patentDatabaseUser, config.patentDatabasePass);

        String query = 
            "select rawinventor.inventor_id, rawlocation.id, " +
            "       city, state, country_transformed " +
            "from rawlocation " +
            "join rawinventor on rawinventor.rawlocation_id = rawlocation.id " +
            "where coalesce(city, state, country_transformed, '') <> '' " +
            "and rawlocation.id = ?";

        LinkedList<RawLocation.RawRecord> rawRecords = new LinkedList<>();

        try (PreparedStatement pstmt = pdb.prepareStatement(query)) {
            locationIds.stream().forEach(id -> {
                try {
                    pstmt.setString(1, id);
                    ResultSet rs = pstmt.executeQuery();

                    if (rs.next()) {
                        String inventorId = rs.getString(1);
                        String locationId = rs.getString(2);
                        String city = rs.getString(3);
                        String state = rs.getString(4);
                        String country = rs.getString(5);
                        rawRecords.add(new RawLocation.RawRecord(locationId, inventorId, city, state, country));
                    }
                    else {
                        System.out.println("No record: " + id);
                    }
                }
                catch (SQLException e) {
                    System.out.println("*** A SQLException occurred.");
                }
            });
        }

        System.out.println(String.format("Got %d raw records\n", rawRecords.size()));

        List<RawLocation.Record> rawLocations =
            rawRecords.stream().map(RawLocation.Record::new).collect(Collectors.toList());

        Disambiguator.disambiguate(
                conn,
                rawLocations,
                config.googleConfidenceThreshold,
                config.fuzzyMatchThreshold);

        System.out.print("Saving results to database... ");
        App.prepareResultTable(conn);
        App.saveResults(conn, rawLocations);
        System.out.println("DONE");

        System.exit(0);
    }
}
