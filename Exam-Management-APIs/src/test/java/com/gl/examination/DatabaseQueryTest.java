package com.gl.examination;

import org.junit.jupiter.api.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DatabaseQueryTest {

    static Connection connection;

    @BeforeAll
    static void init() throws SQLException {
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/survey_db", "root", "");
    }

    @Test
    @Order(1)
    void testUserLoginTableExists() throws SQLException {
        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet rs = dbMeta.getTables(null, null, "UserLogin", null);
        assertTrue(rs.next(), "UserLogin table should exist");
    }

    @Test
    @Order(2)
    void testUserDetailsTableExists() throws SQLException {
        DatabaseMetaData dbMeta = connection.getMetaData();
        ResultSet rs = dbMeta.getTables(null, null, "UserDetails", null);
        assertTrue(rs.next(), "UserDetails table should exist");
    }

    @Test
    @Order(3)
    void testUserLoginTableStructure() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("DESCRIBE UserLogin");

        assertColumn(rs, "id", "int", "NO", "PRI");
        assertColumn(rs, "user_name", "varchar(100)", "YES", "");
        assertColumn(rs, "email_id", "varchar(100)", "YES", "");
        assertColumn(rs, "password", "varchar(100)", "YES", "");
    }

    @Test
    @Order(4)
    void testUserDetailsTableStructure() throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("DESCRIBE UserDetails");

        assertColumn(rs, "id", "int", "NO", "PRI");
        assertColumn(rs, "userLogin_id", "int", "YES", "MUL");
        assertColumn(rs, "gender", "varchar(10)", "YES", "");
        assertColumn(rs, "city", "varchar(50)", "YES", "");
        assertColumn(rs, "mobile_number", "varchar(20)", "YES", "");
        assertColumn(rs, "zipcode", "varchar(10)", "YES", "");
    }

    @Test
    @Order(5)
    void testUserLoginData() throws SQLException {
        String query = "SELECT * FROM UserLogin ORDER BY id";
        ResultSet rs = connection.createStatement().executeQuery(query);

        Object[][] expected = {
                {1, "Mansi", "mansi@example.com", "pass123"},
                {2, "princy", "princy@example.com", "pass456"},
                {3, "raj", "raj@example.com", "pass789"},
                {4, "mohit", "mohit@example.com", "pass000"},
                {5, "sara", "sara@example.com", "pass999"}
        };

        int index = 0;
        while (rs.next()) {
            assertEquals(expected[index][0], rs.getInt("id"));
            assertEquals(((String) expected[index][1]).toLowerCase(), rs.getString("user_name").toLowerCase());
            assertEquals(((String) expected[index][2]).toLowerCase(), rs.getString("email_id").toLowerCase());
            assertEquals(((String) expected[index][3]).toLowerCase(), rs.getString("password").toLowerCase());
            index++;
        }
        assertEquals(expected.length, index, "All 5 rows should be present");
    }

    @Test
    @Order(6)
    void testUserDetailsData() throws SQLException {
        String query = "SELECT * FROM UserDetails ORDER BY id";
        ResultSet rs = connection.createStatement().executeQuery(query);

        Object[][] expected = {
                {101, 2, "F", "Mumbai", "9876543210", "400001"},
                {102, 1, "F", "Delhi", "9998887776", "110001"},
                {103, 3, "M", "Pune", "9988776655", "411001"},
                {104, 4, "M", "Chennai", "9877612345", "600001"},
                {105, 5, "F", "Delhi", "8888777666", "110002"}
        };

        int index = 0;
        while (rs.next()) {
            assertEquals(expected[index][0], rs.getInt("id"));
            assertEquals(expected[index][1], rs.getInt("userLogin_id"));
            assertEquals(((String) expected[index][2]).toLowerCase(), rs.getString("gender").toLowerCase());
            assertEquals(((String) expected[index][3]).toLowerCase(), rs.getString("city").toLowerCase());
            assertEquals(((String) expected[index][4]).toLowerCase(), rs.getString("mobile_number").toLowerCase());
            assertEquals(((String) expected[index][5]).toLowerCase(), rs.getString("zipcode").toLowerCase());
            index++;
        }
        assertEquals(expected.length, index, "All 5 rows should be present");
    }

    @Test
    @Order(7)
    void testQuery1_OutputMatches() throws Exception {
        List<String> queries = readQueriesFromFile();
        assertTrue(queries.size() >= 1, "Query 1 not found in queries.sql");

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(queries.get(0));

        List<Integer> expectedIds = Arrays.asList(102, 105);
        List<Integer> actualIds = new ArrayList<>();

        while (rs.next()) {
            actualIds.add(rs.getInt("id"));
        }

        assertEquals(expectedIds, actualIds, "Query 1 result mismatch (city = 'Delhi')");
    }

    @Test
    @Order(8)
    void testQuery2_OutputMatches() throws Exception {
        List<String> queries = readQueriesFromFile();
        assertTrue(queries.size() >= 2, "Query 2 not found in queries.sql");

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(queries.get(1));

        List<Integer> expectedUserIds = Arrays.asList(3, 4);
        List<Integer> actualUserIds = new ArrayList<>();

        while (rs.next()) {
            actualUserIds.add(rs.getInt("id"));
        }

        assertEquals(expectedUserIds, actualUserIds, "Query 2 result mismatch (gender = 'M')");
    }

    @Test
    @Order(9)
    void testQuery3_OutputMatches() throws Exception {
        List<String> queries = readQueriesFromFile();
        assertTrue(queries.size() >= 3, "Query 3 not found in queries.sql");

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(queries.get(2));

        Set<String> expectedUsernames = new HashSet<>(Arrays.asList("mansi", "princy"));
        Set<String> actualUsernames = new HashSet<>();

        while (rs.next()) {
            actualUsernames.add(rs.getString("user_name").toLowerCase());
        }

        assertEquals(expectedUsernames, actualUsernames, "Query 3 result mismatch (users: Mansi, princy)");
    }

    // Utility method
    void assertColumn(ResultSet rs, String columnName, String dataType, String nullable, String key) throws SQLException {
        boolean found = false;
        while (rs.next()) {
            if (rs.getString("Field").equalsIgnoreCase(columnName)) {
                found = true;
                assertEquals(dataType.toLowerCase(), rs.getString("Type").toLowerCase(), "Data type mismatch for " + columnName);
                assertEquals(nullable, rs.getString("Null"), "Nullability mismatch for " + columnName);
                assertEquals(key, rs.getString("Key"), "Key mismatch for " + columnName);
                break;
            }
        }
        assertTrue(found, "Column " + columnName + " should be present");
    }

    private List<String> readQueriesFromFile() throws IOException {
        File file = new File("src/main/resources/queries.sql");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        List<String> queries = new ArrayList<>();

        StringBuilder currentQuery = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) continue;
            currentQuery.append(line.trim()).append(" ");
            if (line.trim().endsWith(";")) {
                queries.add(currentQuery.toString().replace(";", "").trim());
                currentQuery.setLength(0);
            }
        }
        reader.close();
        return queries;
    }

    @AfterAll
    static void teardown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
