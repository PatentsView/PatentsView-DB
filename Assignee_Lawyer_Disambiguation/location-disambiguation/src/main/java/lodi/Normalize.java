package lodi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import lodi.App.Configuration;

import org.apache.commons.lang3.time.StopWatch;

public class Normalize {
	
	public static void main(String[] args)
	throws ClassNotFoundException, SQLException
	{
        if (args.length < 1) {
            System.out.println("Usage: Need to specify configuration file");
            System.exit(1);
        }

        Configuration config = App.loadConfiguration(args[0]);
        System.out.println("Using configuration:");
        System.out.println(config);

        Class.forName("com.mysql.jdbc.Driver");
        Class.forName("org.sqlite.JDBC");

        Connection conn = DriverManager.getConnection(config.locationDatabase);
        conn.setAutoCommit(false);
        
        DriverManager.setLoginTimeout(10);
        Connection pdb = DriverManager.getConnection(
                config.patentDatabase, config.patentDatabaseUser, config.patentDatabasePass);
        pdb.setAutoCommit(false);
        
        // get total number of records
        Statement stmt = pdb.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from rawlocation where coalesce(city, state, country_transformed, '') <> ''");
        rs.next();
        int total = rs.getInt(1);
        
        // get total number of inventors
        rs = stmt.executeQuery(
        		"select count(distinct inventor_id) " +
        		"from rawinventor " +
        		"join rawlocation on rawlocation.id = rawinventor.rawlocation_id " +
        		"where coalesce(city, state, country_transformed, '') <> '' ");
        rs.next();
        int totalInventors = rs.getInt(1);
        
        double patentsPerInventor = (total + 0.0) / totalInventors;
        int inventorChunkSize = (int)(App.CHUNK / patentsPerInventor);
        int nBreaks = totalInventors / inventorChunkSize;
        
        System.out.format("Total patents = %d\n", total);
        System.out.format("Total inventors = %d\n", totalInventors);
        System.out.format("Patents per inventor = %f\n", patentsPerInventor);
        System.out.format("Inventor chunk size = %d\n", inventorChunkSize);
        System.out.format("Number of breaks = %d\n", nBreaks);
        
        // get inventor breaks
        ArrayList<String> idBreaks = new ArrayList<>();
        
        rs = stmt.executeQuery(
        		"select distinct inventor_id " +
        		"from rawinventor " +
        		"join rawlocation on rawlocation.id = rawinventor.rawlocation_id " +
        		"where coalesce(city, state, country_transformed, '') <> '' " +
        		"and inventor_id is not null " +
        		"order by 1");
        
        int i = 0;
        while(rs.next()) {
        	if (i % inventorChunkSize == 0)
        		idBreaks.add(rs.getString(1));
        	
        	i++;
        }
        
        // this "inventor ID" should come after all valid inventor IDs in sort order
        idBreaks.add("ZZZZZZZZZZZZZZZZZZZZ"); 
        
        System.out.print("Loading cities table... ");
        Cities cities = new Cities(conn);
        System.out.format("got %d records\n", cities.size());
        
        for (i = 0; i < idBreaks.size() - 1; i++) {

        	String leftId = idBreaks.get(i);
        	String rightId = idBreaks.get(i + 1);
	        System.out.format("Requesting records for inventors %s through %s... \n", leftId, rightId);
	        
	        // test to see if the connection is still alive and reconnect if it's not
	        
	        try {
	        	stmt.executeQuery("select 1");
	        }
	        catch(SQLException e) {
	        	pdb = DriverManager.getConnection(
	                    config.patentDatabase, config.patentDatabaseUser, config.patentDatabasePass);
	        	pdb.setAutoCommit(false);
	        }
	        
	        List<Record> locations = load(pdb, cities, leftId, rightId);
	        
	        System.out.format("(got %d)\n", locations.size());
	
	        StopWatch watch = new StopWatch();
	        watch.start();
	        
	        normalize(locations);
	    	
	        watch.stop();
	        System.out.println("Elapsed time: " + watch);
	        
	        System.out.println("Updating database with new changes...");
	        
	        try {
	        	stmt.executeQuery("select 1");
	        }
	        catch(SQLException e) {
	        	pdb = DriverManager.getConnection(
	                    config.patentDatabase, config.patentDatabaseUser, config.patentDatabasePass);
	        	pdb.setAutoCommit(false);
	        }
	        
	        String sql = "update rawlocation set location_id = ? where rawlocation_id = ?";
	        PreparedStatement pstmt = pdb.prepareStatement(sql);
	        
	        for (Record loc: locations) {
	        	if (!loc.update)
	        		continue;
	        	
	        	pstmt.setInt(1, loc.linkedCity.id);
	        	pstmt.setString(2, loc.locationId);
	        	pstmt.executeUpdate();
	        }
	        
	        pdb.commit();
	        pstmt.close();
        }
        
        System.out.println("DONE");
	}
	
	public static class Record {
		public final String locationId;
        public final String inventorId;
        public Cities.Record linkedCity;
        public boolean update;
        
        public Record(String locationId, String inventorId, Cities.Record city) {
        	this.locationId = locationId;
        	this.inventorId = inventorId;
        	this.linkedCity = city;
        	
        	update = false;
        }
	}
	
    public static List<Record> load(
            Connection conn,
            Cities cities,
            String leftId,
            String rightId) 
        throws SQLException
    {
    	LinkedList<Record> result = new LinkedList<>();
    	
        String sql = 
                "select inventor_id, rawlocation_id, location_id " +
                "from rawlocation " +
                "join rawinventor on rawinventor.rawlocation_id = rawlocation.id " +
                "where location_id is not null " +
                "and ? <= inventor_id and inventor_id < ? ";
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, leftId);
            pstmt.setString(2, rightId);
            ResultSet rs = pstmt.executeQuery();
            loadResultSet(result, rs, cities);
        }
    	
    	return result;
    }
    
    protected static void loadResultSet(List<Record> list, ResultSet rs, Cities cities)
    throws SQLException
    {
        while (rs.next()) {
            String inventorId = rs.getString(1);
            String locationId = rs.getString(2);
            int cityId = rs.getInt(3);
            Cities.Record linkedCity = cities.get(cityId);
            
            list.add(new Record(locationId, inventorId, linkedCity));
        }
    }
    
    public static void normalize(List<Record> rawLocations)
    {        
        // normalize inventor portfolios
        
	    System.out.println("Normalize inventor portfolios");
	      
	    ConcurrentMap<String, List<Record>> inventors =
		    rawLocations
		    .parallelStream()
		    .filter(loc -> loc.inventorId != null)
		    .collect(Collectors.groupingByConcurrent(loc -> loc.inventorId));
	      
	    for (String inventorId: inventors.keySet()) {
	      	normalizeInventor(inventorId, inventors.get(inventorId));
	    }
    }
    
    /**
     * Normalize locations in inventor portfolios. If the same city name appears multiple
     * times for different US states, consolidate to the dominant state.
     */
    public static void normalizeInventor(String inventorId, List<Record> records) {
        // group rawlocation records by city

        Map<Cities.Record, List<Record>> map =
            records.stream()
            .filter(r -> r.linkedCity != null && "US".equalsIgnoreCase(r.linkedCity.country))
            .collect(Collectors.groupingBy(r -> r.linkedCity));

        // group the linked city records by city name

        Map<String, List<Cities.Record>> cities =
            map.keySet().stream()
            .filter(c -> c.country.equalsIgnoreCase("US") && c.city != null)
            .collect(Collectors.groupingBy(c -> c.city));

        for (String cityName: cities.keySet()) {
            // Find the US state with the most instances of this city name33

            List<Cities.Record> cityStates = 
                cities.get(cityName).stream()
                .sorted((x, y) -> map.get(y).size() - map.get(x).size())
                .collect(Collectors.toList());

            if (cityStates.size() > 1) {
                Cities.Record first = cityStates.get(0);
                Cities.Record second = cityStates.get(1);

                if (map.get(first).size() == map.get(second).size()) {
                    System.err.format("Can't disambiguate inventor portfolio for %s\n", inventorId);
                }
                else {
	                // re-link raw-locations to first
	                cityStates.stream()
	                    .skip(1)
	                    .forEach(city -> {
	                        map.get(city).stream().forEach(loc -> { loc.linkedCity = first; loc.update = true; });
	                    });
                }
            }
        }
    }
}
