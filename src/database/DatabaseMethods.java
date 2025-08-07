/*
 * Group members: YOUR NAMES HERE
 * Instructions: For Project 2, implement all methods in this class, and test to confirm they behave as expected when the program is run.
 */
package database;

import java.sql.*;
import java.util.*;

import dataClasses.*;
import dataClasses.Driver;

public class DatabaseMethods {

    private Connection conn;

    public DatabaseMethods(Connection conn) {
        this.conn = conn;
    }

    /*
   * Accepts: Nothing
   * Behaviour: Retrieves information about all accounts
   * Returns: List of account objects
   */
  public ArrayList<Account> getAllAccounts() throws SQLException {
    ArrayList<Account> accounts = new ArrayList<>();

    String query = """
          SELECT a.FIRST_NAME, a.LAST_NAME, addr.STREET, addr.CITY, addr.PROVINCE, addr.POSTAL_CODE, a.PHONE_NUMBER, a.EMAIL, a.BIRTHDATE,
          CASE WHEN p.ID IS NOT NULL THEN true ELSE false END AS IS_PASSENGER,
          CASE WHEN d.ID IS NOT NULL THEN true ELSE false END AS IS_DRIVER
          FROM accounts a
          JOIN addresses addr
          ON a.ADDRESS_ID = addr.ID
          LEFT JOIN passengers p
          ON a.ID = p.ID
          LEFT JOIN drivers d
          ON a.ID = d.ID
        """;
    try (PreparedStatement ps = conn.prepareStatement(query);
        ResultSet rs = ps.executeQuery();) {
      while (rs.next()) {
        Account acc = new Account(
            rs.getString("FIRST_NAME"),
            rs.getString("LAST_NAME"),
            rs.getString("STREET"),
            rs.getString("CITY"),
            rs.getString("PROVINCE"),
            rs.getString("POSTAL_CODE"),
            rs.getString("PHONE_NUMBER"),
            rs.getString("EMAIL"),
            rs.getString("BIRTHDATE"),
            rs.getBoolean("IS_PASSENGER"),
            rs.getBoolean("IS_DRIVER"));
        accounts.add(acc);
      }
      return accounts;
    }
  }

  /*
   * Accepts: Email address of driver
   * Behaviour: Calculates the average rating over all rides performed by the
   * driver specified by the email address
   * Returns: The average rating value
   */
  public double getAverageRatingForDriver(String driverEmail) throws SQLException {
    double averageRating = 0.0;

    String query = """
         SELECT d.ID, a.EMAIL, AVG(r.RATING_FROM_PASSENGER) AS AVERAGE_RATING
         FROM drivers d
         INNER JOIN accounts a
         ON d.ID = a.ID
         INNER JOIN rides r
         ON d.ID = r.DRIVER_ID
         WHERE a.EMAIL = ?
        """;

    try (PreparedStatement ps = conn.prepareStatement(query);) {
      ps.setString(1, driverEmail);
      try (ResultSet rs = ps.executeQuery();) {
        while (rs.next()) {
          averageRating = rs.getInt("AVERAGE_RATING");
        }
      }

      return averageRating;
    }
  }

  /*
   * Accepts: Account details, and passenger and driver specific details.
   * Passenger or driver details could be
   * null if account is only intended for one type of use.
   * Behaviour:
   *  - Insert new account using information provided in Account object
   *  - For non-null passenger/driver details, insert the associated data into the relevant tables
   * Returns: Nothing 
     */
    public void createAccount(Account account, Passenger passenger, Driver driver) throws SQLException {
        // TODO: Implement
        // Hint: Use the available insertAccount, insertPassenger, and insertDriver methods
    }

    /*
   * Accepts: Account details (which includes address information)
   * Behaviour: Inserts the new account, as well as the account's address if it doesn't already exist. The new/existing address should
   * be linked to the account
   * Returns: Id of the new account 
     */
    public int insertAccount(Account account) throws SQLException {
        int accountId = -1;

        // TODO: Implement
        // Hint: Use the insertAddressIfNotExists method
        return accountId;
    }

    /*
   * Accepts: Passenger details (should not be null), and account id for the passenger
   * Behaviour: Inserts the new passenger record, correctly linked to the account id
   * Returns: Id of the new passenger 
     */
    public int insertPassenger(Passenger passenger, int accountId) throws SQLException {
        // TODO: Implement

        return accountId;
    }

    /*
   * Accepts: Driver details (should not be null), and account id for the driver
   * Behaviour: Inserts the new driver and driver's license record, correctly linked to the account id
   * Returns: Id of the new driver 
     */
    public int insertDriver(Driver driver, int accountId) throws SQLException {
        // TODO: Implement
        // Hint: Use the insertLicense method

        return accountId;
    }

    /*
   * Accepts: Driver's license number and license expiry
   * Behaviour: Inserts the new driver's license record
   * Returns: Id of the new driver's license
     */
    public int insertLicense(String licenseNumber, String licenseExpiry) throws SQLException {
        int licenseId = -1;
        // TODO: Implement

        return licenseId;
    }

    /*
   * Accepts: Address details
   * Behaviour: 
   *  - Checks if an address with these properties already exists.
   *  - If it does, gets the id of the existing address.
   *  - If it does not exist, creates the address in the database, and gets the id of the new address
   * Returns: Id of the address
   */
  public int insertAddressIfNotExists(Address address) throws SQLException {
    int addressId = -1;

    int id = address.getId();
    String street = address.getStreet();
    String city = address.getCity();
    String province = address.getProvince();
    String postalCode = address.getPostalCode();

    String checkQuery = """
          SELECT *
          FROM addresses
          WHERE ID = ? AND STREET = ? AND CITY = ? AND PROVINCE = ? AND POSTAL_CODE = ?
        """;

    try (PreparedStatement psCheck = conn.prepareStatement(checkQuery);) {
      psCheck.setInt(1, id);
      psCheck.setString(2, street);
      psCheck.setString(3, city);
      psCheck.setString(4, province);
      psCheck.setString(5, postalCode);
      try (ResultSet rs = psCheck.executeQuery();) {
        if (rs.next()) {
          addressId = id;
        } else {
          String insertQuery = """
               INSERT INTO addresses
               VALUES(?, ?, ?, ?, ?);
              """;

          try (PreparedStatement psInsert = conn.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS);) {
            psInsert.setInt(1, id);
            psInsert.setString(2, street);
            psInsert.setString(3, city);
            psInsert.setString(4, province);
            psInsert.setString(5, postalCode);
            psInsert.executeUpdate();

            try (ResultSet generatedKeys = psInsert.getGeneratedKeys();) {
              if (generatedKeys.next()) {
                addressId = generatedKeys.getInt(1);
              }
            }
          }
        }
      }
    }

    return addressId;
  }

  /*
   * Accepts: Name of new favourite destination, email address of the passenger,
   * and the id of the address being favourited
   * Behaviour: Finds the id of the passenger with the email address, then inserts
   * the new favourite destination record
   * Returns: Nothing
     */
    public void insertFavouriteDestination(String favouriteName, String passengerEmail, int addressId)
            throws SQLException {
        // TODO: Implement
    }

    /*
   * Accepts: Email address
   * Behaviour: Determines if a driver exists with the provided email address
   * Returns: True if exists, false if not
     */
    public boolean checkDriverExists(String email) throws SQLException {
        String query = """
        SELECT d.ID
        FROM drivers d
        JOIN accounts a ON d.ID = a.ID
        WHERE a.EMAIL = ?
        """;

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, email);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    /*
   * Accepts: Email address
   * Behaviour: Determines if a passenger exists with the provided email address
   * Returns: True if exists, false if not
     */
    public boolean checkPassengerExists(String email) throws SQLException {
        String query = """
        SELECT p.ID
        FROM passengers p
        JOIN accounts a ON p.ID = a.ID
        WHERE a.EMAIL = ?
        """;

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, email);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }


    /*
   * Accepts: Email address of passenger making request, id of dropoff address, requested date/time of ride, and number of passengers
   * Behaviour: Inserts a new ride request, using the provided properties
   * Returns: Nothing
     */
    public void insertRideRequest(String passengerEmail, int dropoffLocationId, String date, String time,
            int numberOfPassengers) throws SQLException {

        int passengerId = this.getPassengerIdFromEmail(passengerEmail);
        int pickupAddressId = this.getAccountAddressIdFromEmail(passengerEmail);

        String insertQuery = """
        INSERT INTO ride_requests (PASSENGER_ID, PICKUP_LOCATION_ID, PICKUP_DATE, PICKUP_TIME, NUMBER_OF_RIDERS, DROPOFF_LOCATION_ID)
        VALUES (?, ?, ?, ?, ?, ?)
        """;

        try (PreparedStatement stmt = conn.prepareStatement(insertQuery)) {
            stmt.setInt(1, passengerId);
            stmt.setInt(2, pickupAddressId);
            stmt.setString(3, date);
            stmt.setString(4, time);
            stmt.setInt(5, numberOfPassengers);
            stmt.setInt(6, dropoffLocationId);

            stmt.executeUpdate();
        }
    }


    /*
   * Accepts: Email address
   * Behaviour: Gets id of passenger with specified email (assumes passenger exists)
   * Returns: Id
     */
    public int getPassengerIdFromEmail(String passengerEmail) throws SQLException {
        int passengerId = -1;

        String query = """
        SELECT p.ID
        FROM passengers p
        JOIN accounts a ON p.ID = a.ID
        WHERE a.EMAIL = ?
        """;
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, passengerEmail);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    passengerId = rs.getInt("ID");
                }
            }
        }

        return passengerId;
    }


    /*
   * Accepts: Email address
   * Behaviour: Gets id of driver with specified email (assumes driver exists)
   * Returns: Id
     */
    public int getDriverIdFromEmail(String driverEmail) throws SQLException {
        int driverId = -1;
        String query = """
        SELECT d.ID
        FROM drivers d
        JOIN accounts a ON d.ID = a.ID
        WHERE a.EMAIL = ?
    """;
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, driverEmail);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    driverId = rs.getInt("ID");
                }
            }
        }

        return driverId;
    }

    /*
   * Accepts: Email address
   * Behaviour: Gets the id of the address tied to the account with the provided email address
   * Returns: Address id
     */
    public int getAccountAddressIdFromEmail(String email) throws SQLException {
        int addressId = -1;

        String query = "SELECT ADDRESS_ID FROM accounts WHERE EMAIL = ?";

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, email);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    addressId = rs.getInt("ADDRESS_ID");
                }
            }
        }
        return addressId;
    }


    /*
   * Accepts: Email address of passenger
   * Behaviour: Gets a list of all the specified passenger's favourite destinations
   * Returns: List of favourite destinations
     */
    public ArrayList<FavouriteDestination> getFavouriteDestinationsForPassenger(String passengerEmail)
            throws SQLException {
        ArrayList<FavouriteDestination> favouriteDestinations = new ArrayList<>();

        String query = """
        SELECT fl.NAME, a.ID AS addressId, a.STREET, a.CITY, a.PROVINCE, a.POSTAL_CODE
        FROM favourite_locations fl
        JOIN addresses a ON fl.LOCATION_ID = a.ID
        JOIN passengers p ON fl.PASSENGER_ID = p.ID
        JOIN accounts acc ON p.ID = acc.ID
        WHERE acc.EMAIL = ?
    """;

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, passengerEmail);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("NAME");
                    int addressId = rs.getInt("addressId");
                    String street = rs.getString("STREET");
                    String city = rs.getString("CITY");
                    String province = rs.getString("PROVINCE");
                    String postalCode = rs.getString("POSTAL_CODE");

                    FavouriteDestination destination = new FavouriteDestination(
                            name, addressId, street, city, province, postalCode
                    );
                    favouriteDestinations.add(destination);
                }
            }
        }

        return favouriteDestinations;
    }


    /*
   * Accepts: Nothing
   * Behaviour: Gets a list of all uncompleted ride requests (i.e. requests without an associated ride record)
   * Returns: List of all uncompleted rides
     */
    public ArrayList<RideRequest> getUncompletedRideRequests() throws SQLException {
        ArrayList<RideRequest> uncompletedRideRequests = new ArrayList<>();

        String query = """
        SELECT rr.ID,
               a.FIRST_NAME,
               a.LAST_NAME,
               pickup.STREET AS pickup_street,
               pickup.CITY AS pickup_city,
               dropoff.STREET AS dropoff_street,
               dropoff.CITY AS dropoff_city,
               rr.PICKUP_DATE,
               rr.PICKUP_TIME
        FROM ride_requests rr
        JOIN passengers p ON rr.PASSENGER_ID = p.ID
        JOIN accounts a ON p.ID = a.ID
        JOIN addresses pickup ON rr.PICKUP_LOCATION_ID = pickup.ID
        JOIN addresses dropoff ON rr.DROPOFF_LOCATION_ID = dropoff.ID
        LEFT JOIN rides r ON rr.ID = r.REQUEST_ID
        WHERE r.REQUEST_ID IS NULL
    """;

        try (PreparedStatement stmt = conn.prepareStatement(query); ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                RideRequest request = new RideRequest(
                        rs.getInt("ID"),
                        rs.getString("FIRST_NAME"),
                        rs.getString("LAST_NAME"),
                        rs.getString("pickup_street"),
                        rs.getString("pickup_city"),
                        rs.getString("dropoff_street"),
                        rs.getString("dropoff_city"),
                        rs.getString("PICKUP_DATE"),
                        rs.getString("PICKUP_TIME")
                );
                uncompletedRideRequests.add(request);
            }
        }

        return uncompletedRideRequests;
    }


    /*
   * Accepts: Ride details
   * Behaviour: Inserts a new ride record
   * Returns: Nothing
     */
    public void insertRide(Ride ride) throws SQLException {
        // Lấy driverId từ email
        int driverId = getDriverIdFromEmail(ride.getDriverEmail());

        String insertQuery = """
        INSERT INTO rides
        (DRIVER_ID, REQUEST_ID, ACTUAL_START_DATE, ACTUAL_START_TIME,
         ACTUAL_END_DATE, ACTUAL_END_TIME, DISTANCE, CHARGE,
         RATING_FROM_DRIVER, RATING_FROM_PASSENGER)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

        try (PreparedStatement stmt = conn.prepareStatement(insertQuery)) {
            stmt.setInt(1, driverId);
            stmt.setInt(2, ride.getRideRequestId());
            stmt.setString(3, ride.getStartDate());
            stmt.setString(4, ride.getStartTime());
            stmt.setString(5, ride.getEndDate());
            stmt.setString(6, ride.getEndTime());
            stmt.setDouble(7, ride.getDistance());
            stmt.setDouble(8, ride.getCharge());
            stmt.setInt(9, ride.getRatingFromDriver());
            stmt.setInt(10, ride.getRatingFromPassenger());

            stmt.executeUpdate();
        }
    }

}
