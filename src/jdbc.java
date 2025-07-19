import java.sql.*;

public class jdbc {
    private static final String CREATE_SQL = "CREATE TABLE IF NOT EXISTS student(id int NOT NULL PRIMARY KEY, name varchar(20), address varchar(20))";
    private static final String INSERT_SQL = "INSERT INTO student(id, name, address) VALUES (?, ?, ?)";
    private static final String SELECT_SQL = "SELECT * FROM student";
    private static final String SELECT_IN_PARA = "SELECT * FROM getId(?)";

    public static void createTable(Connection con) throws SQLException {
        try (PreparedStatement statement = con.prepareStatement(CREATE_SQL)) {
            statement.execute();
        }
    }

    public static void insertStudent(Connection con, int id, String name, String address) throws SQLException {
        try (PreparedStatement statement = con.prepareStatement(INSERT_SQL)) {
            statement.setInt(1, id);
            statement.setString(2, name);
            statement.setString(3, address);
            int result = statement.executeUpdate();
            System.out.println("\n" + result + " row/s affected");
        }
    }

    public static void selectQuery(Connection con) throws SQLException{
        try(PreparedStatement statement = con.prepareStatement(SELECT_SQL)){
            ResultSet rs = statement.executeQuery();
            while (rs.next()){
                System.out.println(rs.getInt("id") + " " + rs.getString("name") + " " + rs.getString("address"));
            }
            System.out.println();
        }
    }

    public static void storedProcedure(Connection con,int id) throws SQLException{
        try(PreparedStatement statement = con.prepareStatement(SELECT_IN_PARA)){
            statement.setInt(1,id);
            ResultSet rs = statement.executeQuery();
            while(rs.next()){
                System.out.println(rs.getInt("id") + " " + rs.getString("name") + " " + rs.getString("address"));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://localhost:5432/jdbcdemo";
        String username = "postgres";
        String password = "1234";

        try (Connection con = DriverManager.getConnection(url, username, password)) {
            System.out.println("Connection Established");
            createTable(con);
            System.out.println("Table created");
            // insertStudent(con, 1, "John", "Madurai");
            // insertStudent(con,2,"Naveen","Tirunelveli");
            //insertStudent(con,3,"Ashwin","Tenkasi");
            //selectQuery(con);
            storedProcedure(con,2);
        }
    }


}

