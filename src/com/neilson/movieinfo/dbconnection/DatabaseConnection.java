package com.neilson.movieinfo.dbconnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * This is a class for Database connection. Registering driver and setting up
 * dbUrl,user,password to get connection from mySQL database.
 *
 */

public class DatabaseConnection {

	private static DatabaseConnection instance;
	private Connection connection;
	public String dbUrl = null;
	public final String user = "root";
	public final String password = "Gracenote@123";
	public final String driver = "com.mysql.cj.jdbc.Driver";

	/* @param task - to get appropriate schema through dbUrl */
	private DatabaseConnection(int task) throws SQLException {

		if (task == 1) {
			dbUrl = "jdbc:mysql://127.0.0.1:3306/theater?useSSL=false";
		} else if (task == 2) {
			dbUrl = "jdbc:mysql://127.0.0.1:3306/enhanced_theater?useSSL=false";
		}

		try {
			Class.forName(driver);// This will load the MySQL driver
			// Setting up the connection with the DB
			this.connection = DriverManager.getConnection(dbUrl, user, password);
			// System.out.println("DB connected");

		} catch (ClassNotFoundException ex) {
			System.out.println("DB Connection failed:" + ex.getMessage());
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public static DatabaseConnection getInstance(int task) throws SQLException {

		if (instance == null) {
			instance = new DatabaseConnection(task);
		} else if (instance.getConnection().isClosed()) {
			instance = new DatabaseConnection(task);
		}
		return instance;
	}
}
