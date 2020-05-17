package com.neilson.movieinfo.main;

import java.util.InputMismatchException;
import java.util.Scanner;

import com.neilson.movieinfo.dao.TheaterInfoDAO;
import com.neilson.movieinfo.daoimpl.TheaterInfoDAOImpl;

/**
 * Main class is executing two methods based on the option entered by the user
 * input in console as (1 or 2) . Task 1- To Insert all the data from CSV file
 * into one table. Task 2- To Insert all the data from CSV file into relational
 * tables.
 */

public class Main {

	public static void main(String[] args) {

		System.out.println("Select one of the Task-");
		System.out.println("Task 1- To Insert all the data from CSV file into one table");
		System.out.println("Task 2- To Insert all the data from CSV file into relational tables\n");

		System.out.println("Please enter 1 for task-1 or 2 for task-2:");
		// Using Scanner for Getting Input from User
		Scanner input = new Scanner(System.in);

		try {
			int task = input.nextInt();
			System.out.println("You entered " + task);
			try {
				TheaterInfoDAO dao = new TheaterInfoDAOImpl();
				if (task == 1) {
					dao.insertTheatreInformation(task);
				} else if (task == 2) {
					dao.insertEnhancedTheaterInformation(task);
				} else {
					System.err.println("no task performed for insertion.\nPlease enter correct Integer- 1 or 2");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		} catch (InputMismatchException e) {
			System.err.println("Entered Value is not an Integer. Please enter input as 1 or 2");

		}
	}

}
