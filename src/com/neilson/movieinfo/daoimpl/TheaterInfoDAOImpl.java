package com.neilson.movieinfo.daoimpl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.neilson.movieinfo.dao.TheaterInfoDAO;
import com.neilson.movieinfo.dbconnection.DatabaseConnection;

public class TheaterInfoDAOImpl implements TheaterInfoDAO {

	// all the queries that will be needed while execution
	private static final String insertInfo = "Insert ignore into theater_data values(?,?,?,?,?,?,?,?,?)";
	private static final String insertChainsInfo = "Insert ignore into chains (chain_id,chain_name)  values(?,?)";
	private static final String insertTheaterInfo = "Insert ignore into theater(theater_id,theater_name)  values(?,?)";
	private static final String insertMovieInfo = "Insert ignore into movie (movie_id,movie_title) values(?,?)";
	private static final String insertAttributesInfo = "Insert ignore into attributes (attribute_name) values(?)";
	private static final String insertShowDetailsInfo = "Insert ignore into show_details (chain_id,theater_id,movie_id,show_date,show_time,attribute_id)  values(?,?,?,?,?,?)";
	private static final String selectAttributesId = "Select attribute_id from attributes where attribute_name = ?";
	private static final String selectAttributesIdIfNull = "Select attribute_id from attributes where attribute_name IS NULL OR attribute_name = ?";

	@Override
	public void insertTheatreInformation(int task) throws SQLException {

		BufferedReader csvReader = null;
		String csvFile = "resources/theater_data.csv";// location of the csv file
		String row = "";
		String headerLine = "";

		Connection conn = null;
		PreparedStatement stmt = null;
		try {

			conn = DatabaseConnection.getInstance(task).getConnection();// getting db connection
			stmt = conn.prepareStatement(insertInfo);

			try {
				csvReader = new BufferedReader(new FileReader(csvFile));// reading csv file

				headerLine = csvReader.readLine();// to avoid header line

				// getting csv file rows one by one and set the parameter to prepared statement
				while ((row = csvReader.readLine()) != null) {

					String[] data = row.split(",", -1);// getting all the fields into array by split csv file row

					stmt.setInt(1, Integer.parseInt(data[0]));
					stmt.setString(2, data[1]);
					stmt.setInt(3, Integer.parseInt(data[2]));
					stmt.setString(4, data[3]);
					stmt.setInt(5, Integer.parseInt(data[4]));
					stmt.setString(6, data[5]);
					DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
					Date myDate;
					try {
						myDate = formatter.parse(data[6]);
						java.sql.Date sqlDate = new java.sql.Date(myDate.getTime());
						stmt.setDate(7, sqlDate);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					stmt.setString(8, data[7]);
					// if field is empty string setting null to insert in table
					if (data[8].equals("")) {
						stmt.setString(9, null);
					} else {
						stmt.setString(9, data[8]);
					}

					stmt.addBatch();
				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();

			} catch (IOException e) {
				e.printStackTrace();

			} finally {
				if (csvReader != null) {
					try {
						// System.out.println("closing csv file");
						csvReader.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			stmt.executeBatch();// executing all the queries
			System.out.println("Successfully inserted data into theater_data table.");

		} catch (SQLException e) {
			e.printStackTrace();

		} finally {

			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
			// System.out.println("all the statements and db connection are closed");
		}

	}

	@Override
	public void insertEnhancedTheaterInformation(int task) throws SQLException {

		BufferedReader csvReader = null;
		String csvFile = "resources/theater_data.csv";// location of the csv file
		String row = "";
		String headerLine = "";
		Map<Integer, String> chainsinfo = new HashMap<Integer, String>();
		Map<Integer, String> theaterinfo = new HashMap<Integer, String>();
		Map<Integer, String> movieinfo = new HashMap<Integer, String>();
		Set<String> attributeinfoset = new LinkedHashSet<String>();

		try {
			csvReader = new BufferedReader(new FileReader(csvFile));
			// System.out.println("csv file opened");
			headerLine = csvReader.readLine();// to avoid header line

			while ((row = csvReader.readLine()) != null) {

				String[] data = row.split(",", -1);

				/*
				 * storing (id,name) as (key,value) pair using HashMap for chains, theater,
				 * movie table
				 */
				chainsinfo.put(Integer.parseInt(data[0]), data[1]);
				theaterinfo.put(Integer.parseInt(data[2]), data[3]);
				movieinfo.put(Integer.parseInt(data[4]), data[5]);

				/*
				 * if value for attribute_name is empty string adding null else attribute name
				 * using LinkedHashSet for attribute table
				 */
				if (data[8].equals("")) {
					attributeinfoset.add(null);
				} else {
					attributeinfoset.add(data[8]);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();

		} catch (IOException e) {
			e.printStackTrace();

		} finally {
			if (csvReader != null) {
				try {
					csvReader.close();
					// System.out.println("closing csv file");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		Connection conn = null;
		PreparedStatement insertChainsInfostmt = null, insertTheaterInfostmt = null, insertMovieInfostmt = null,
				insertAttributesInfostmt = null, selectAttributesNameIfNullstmt = null,
				insertShowDetailsInfostmt = null, selectAttributesIdInfostmt = null;
		ResultSet resultSet = null;

		try {

			conn = DatabaseConnection.getInstance(task).getConnection();// getting db connection

			insertChainsInfostmt = conn.prepareStatement(insertChainsInfo);
			insertTheaterInfostmt = conn.prepareStatement(insertTheaterInfo);
			insertMovieInfostmt = conn.prepareStatement(insertMovieInfo);
			insertAttributesInfostmt = conn.prepareStatement(insertAttributesInfo);
			selectAttributesIdInfostmt = conn.prepareStatement(selectAttributesId);
			selectAttributesNameIfNullstmt = conn.prepareStatement(selectAttributesIdIfNull);

			/*
			 * Iterating all the values and setting the parameters in their respective
			 * prepared statement to prepared the queries. Then adding it to the batch to
			 * execute it all at once later on.
			 */

			for (Map.Entry<Integer, String> entry : chainsinfo.entrySet()) {

				insertChainsInfostmt.setInt(1, entry.getKey());
				insertChainsInfostmt.setString(2, entry.getValue());
				insertChainsInfostmt.addBatch();// adding queries
			}
			for (Map.Entry<Integer, String> entry : theaterinfo.entrySet()) {

				insertTheaterInfostmt.setInt(1, entry.getKey());
				insertTheaterInfostmt.setString(2, entry.getValue());
				insertTheaterInfostmt.addBatch();
			}

			for (Map.Entry<Integer, String> entry : movieinfo.entrySet()) {

				insertMovieInfostmt.setInt(1, entry.getKey());
				insertMovieInfostmt.setString(2, entry.getValue());
				insertMovieInfostmt.addBatch();
			}
			for (String attriinfo : attributeinfoset) {
				insertAttributesInfostmt.setString(1, attriinfo);
				insertAttributesInfostmt.addBatch();
			}

			insertChainsInfostmt.executeBatch();
			insertTheaterInfostmt.executeBatch();
			insertMovieInfostmt.executeBatch();
			insertAttributesInfostmt.executeBatch();

			System.out.println("Successfully inserted data in chains, theater, movie, attributes tables");

		} catch (SQLException e) {
			e.printStackTrace();

		}

		try {

			insertShowDetailsInfostmt = conn.prepareStatement(insertShowDetailsInfo);

			try {
				csvReader = new BufferedReader(new FileReader(csvFile));
				// System.out.println("csv file opened");
				headerLine = csvReader.readLine();// to avoid header line

				// getting csv file rows one by one and set the parameter to prepared statement
				while ((row = csvReader.readLine()) != null) {

					String[] data = row.split(",", -1);

					insertShowDetailsInfostmt.setInt(1, Integer.parseInt(data[0]));
					insertShowDetailsInfostmt.setInt(2, Integer.parseInt(data[2]));
					insertShowDetailsInfostmt.setInt(3, Integer.parseInt(data[4]));

					DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
					Date myDate;
					try {
						myDate = formatter.parse(data[6]);
						java.sql.Date sqlDate = new java.sql.Date(myDate.getTime());
						insertShowDetailsInfostmt.setDate(4, sqlDate);
					} catch (ParseException e) {
						e.printStackTrace();
					}

					insertShowDetailsInfostmt.setString(5, data[7]);

					if (data[8].equals("")) {
						selectAttributesNameIfNullstmt.setString(1, null);
						resultSet = selectAttributesNameIfNullstmt.executeQuery();

					} else {
						selectAttributesIdInfostmt.setString(1, data[8]);
						resultSet = selectAttributesIdInfostmt.executeQuery();
					}

					while (resultSet.next()) {
						int attribute_id = resultSet.getInt("attribute_id");
						insertShowDetailsInfostmt.setInt(6, attribute_id);
					}

					insertShowDetailsInfostmt.addBatch();
				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();

			} catch (IOException e) {
				e.printStackTrace();

			} finally {
				if (csvReader != null) {
					try {
						// System.out.println("closing csv file");
						csvReader.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			insertShowDetailsInfostmt.executeBatch();// executing all the queries.
			System.out.println("Successfully inserted data into show_details table.");

		} catch (SQLException e) {
			e.printStackTrace();

		} finally {
			if (insertChainsInfostmt != null)
				insertChainsInfostmt.close();
			if (insertTheaterInfostmt != null)
				insertTheaterInfostmt.close();
			if (insertMovieInfostmt != null)
				insertMovieInfostmt.close();
			if (insertAttributesInfostmt != null)
				insertAttributesInfostmt.close();
			if (selectAttributesIdInfostmt != null)
				selectAttributesIdInfostmt.close();
			if (selectAttributesNameIfNullstmt != null)
				selectAttributesNameIfNullstmt.close();
			if (insertShowDetailsInfostmt != null)
				insertShowDetailsInfostmt.close();
			if (conn != null)
				conn.close();
			// System.out.println("all the statements and db connection are closed");
		}
	}
}