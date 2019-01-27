package pl.progon.pgup;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.exit;

public class DBConnector {
    private String connString;
    private String user;
    private String password;

    public DBConnector(String host, String port, String dbname, String user, String password) {
        this.connString = String.format("jdbc:postgresql://%s:%s/%s", host, port, dbname);
        this.user = user;
        this.password = password;
    }

    private Connection getConnection(){
        Connection dbConnection = null;

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        try {
            dbConnection = DriverManager.getConnection(this.connString, this.user,this.password);
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            exit(0);
        }
        return dbConnection;

    }

    public List<String> getExecutedFiles() throws SQLException {
        String selectTableSQL = "SELECT skrypt FROM public.sys_skrypty";
        Connection connection = null;
        Statement statement = null;
        List<String> result = new ArrayList<>();
        try {
            connection = getConnection();
            statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(selectTableSQL);
            while (rs.next()) {
                result.add(rs.getString("SKRYPT"));
            }
        } finally {
            if (statement != null) {
                statement.close();
            }

            if (connection != null) {
                connection.close();
            }
        }
        return result;
    }

    public void executeScript(String script) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        List<String> result = new ArrayList<>();
        try {
            connection = getConnection();
            statement = connection.createStatement();
            statement.execute(script);
        } finally {
            if (statement != null) {
                statement.close();
            }

            if (connection != null) {
                connection.close();
            }
        }
    }
}
