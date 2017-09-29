package lodi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import lodi.App.Configuration;

public class UploadCodedLocations {

	public final static String DB = "jtokle";
	
	public static void main(String... args)
	throws ClassNotFoundException, SQLException
	{
        if (args.length < 1) {
            System.out.println("Usage: java -jar lodi.jar CONFIG_FILE");
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
        
//        System.out.println("Create location table...");
//        createLocationTable(conn, pdb);
        
        System.out.println("Create croswalk table...");
        createXwalkTable(conn, pdb);
        
        System.out.println("DONE");
	}
	
	public static void createLocationTable(Connection geodb, Connection patdb)
	throws SQLException
	{
		Statement geost = geodb.createStatement();
		Statement patst = patdb.createStatement();
		
		patst.execute(String.format("drop table if exists %s.location__", DB));
		patst.execute(String.format(
				"create table %s.location__ (" +
				"id int primary key, " +
				"city varchar(128), " +
				"state varchar(100), " +
				"country varchar(10), " +
				"latitude float, " +
				"longitude float, " +
				"index (country), " +
				"index (state), " +
				"index (city, state, country), " +
				"index (latitude, longitude))",
				DB));
		
		ResultSet rs = geost.executeQuery(
				"select distinct id, cities.city, cities.region state, cities.country, latitude, longitude " +
				"from cities " +
				"join coded_locations on coded_locations.city_id = cities.id"
				);
		
		PreparedStatement prep = patdb.prepareStatement(
				String.format("insert into %s.location__ values (?, ?, ?, ?, ?, ?)", DB));
		
		while (rs.next()) {
			prep.setInt(1, rs.getInt(1));
			prep.setString(2, rs.getString(2));
			prep.setString(3, rs.getString(3));
			prep.setString(4, rs.getString(4));
			prep.setFloat(5, rs.getFloat(5));
			prep.setFloat(6,  rs.getFloat(6));
			
			prep.execute();
		}
		
		patdb.commit();
	}
	
	public static void createXwalkTable(Connection geodb, Connection patdb)
	throws SQLException
	{
		Statement geost = geodb.createStatement();
		Statement patst = patdb.createStatement();
		
		patst.execute(String.format("drop table if exists %s.location_xwalk", DB));
		patst.execute(String.format(
				"create table %s.location_xwalk (" +
				"rawlocation_id varchar(128) not null, " +
				"location_id int not null, " +
				"primary key (rawlocation_id, location_id), " +
				"foreign key (location_id) references location__ (id))",
				DB));
		
		ResultSet rs = geost.executeQuery(
				"select location_id, city_id " +
				"from coded_locations " +
				"where city_id is not null"
				);
		
		PreparedStatement prep = patdb.prepareStatement(
				String.format("insert into %s.location_xwalk values (?, ?)", DB));
		
		while (rs.next()) {
			prep.setString(1, rs.getString(1));
			prep.setInt(2, rs.getInt(2));
			
			prep.execute();
		}
		
		patdb.commit();
	}
}
