package com.bridgelab.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

public class DBDemo {
	
	
	public static void main(String[] args) {
		Connection connection;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			System.out.println("Driver Loaded!");
			String dbURL = "jdbc:mysql://localhost:3306/payroll_services_";
			System.out.println("Connecting to dbURL... " + dbURL);
			String username = "root";
			String password = "1234";
			connection = DriverManager.getConnection(dbURL,username, password);
			System.out.println("Connection is successful!! " + connection);
		} 
		catch (Exception e) {		
			throw new IllegalStateException("Cannot find the driver in the classpath!", e);
		}
		listDrivers();
	}
	private static void listDrivers() {
		Enumeration<Driver> driverList = DriverManager.getDrivers();
		while(driverList.hasMoreElements()) {
			Driver driverClass = (Driver) driverList.nextElement();
			System.out.println("  "+ driverClass.getClass().getName());
		}	
	}

}
