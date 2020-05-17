package com.neilson.movieinfo.dao;

import java.sql.SQLException;

public interface TheaterInfoDAO {

	/**
	 * This method is getting connection from mySQL database schema "theater". It is
	 * reading CSV file and inserting all the records into the table "theater_data".
	 * 
	 * @param task
	 *            Pass this parameter into DatabaseConnection class to select the
	 *            appropriate schema while connecting to the database.
	 */

	public void insertTheatreInformation(int task) throws SQLException;

	/**
	 * This method is getting connection from mySQL database schema
	 * "enhanced_theater". It is reading CSV file and inserting all the records into
	 * relational tables which are "theater","chains","attributes", "movie" and
	 * "show_details".
	 * 
	 * @param task
	 *            Pass this parameter into DatabaseConnection class to select the
	 *            appropriate schema while connecting to the database.
	 */

	public void insertEnhancedTheaterInformation(int task) throws SQLException;

}
