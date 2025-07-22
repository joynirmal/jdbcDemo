import java.sql.*;

public class jdbc {
    private static final String CREATE_SQL = "CREATE TABLE IF NOT EXISTS student(id int NOT NULL PRIMARY KEY, name varchar(20), address varchar(20))";
    private static final String INSERT_SQL = "INSERT INTO student(id, name, address) VALUES (?, ?, ?)";
    private static final String SELECT_IN_PARAMETER = "SELECT * FROM getStudent(?)";
    private static final String UPDATE_IN_IN =  "SELECT updateStud(?,?)";
    private static final String SELECT_EMPTY = "SELECT * FROM getNames()";
    private static final String SELECT_IN_OUT = "{CALL getName(?,?)}";
    private static final String DROP_SQL = "DROP TABLE IF EXISTS student";
    private static final String VIEW_SQL = "SELECT * FROM student";

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
            System.out.println(result + " row/s affected");
        }
        System.out.println();
    }


    public static void storedFunction(Connection con,int id) throws SQLException{
        try(PreparedStatement statement = con.prepareStatement(SELECT_IN_PARAMETER)){
            statement.setInt(1,id);
            ResultSet rs = statement.executeQuery();
            while(rs.next()) {
                System.out.println(rs.getInt("id") + " " + rs.getString("name") + " " + rs.getString("address"));
            }
        }
    }

    public static void updateFunction(Connection con,int id, String name) throws SQLException{
        try(PreparedStatement Statement = con.prepareStatement(UPDATE_IN_IN)){
            Statement.setInt(1,id);
            Statement.setString(2,name);
            ResultSet rs = Statement.executeQuery();
            while (rs.next()) {
                int rows = rs.getInt(1);
                System.out.println("\n" + rows + " row/s affected");
            }
        }
    }

    public static void namesFunction(Connection con) throws  SQLException{
        try(PreparedStatement statement = con.prepareStatement(SELECT_EMPTY)) {
            ResultSet rs = statement.executeQuery();
            System.out.println("Names: ");
            while (rs.next()){
                String names = rs.getString("name");
                System.out.println(names);
            }
        }
    }

    public static void namesWith_inout(Connection con, int id) throws SQLException{
        try(CallableStatement callableStatement = con.prepareCall(SELECT_IN_OUT)){
            callableStatement.setInt(1,id);
            callableStatement.registerOutParameter(2, Types.VARCHAR);
            callableStatement.execute();
            String name = callableStatement.getString(2);
            System.out.println(name);
        }
    }

    public static void dropTable(Connection con) throws SQLException{
        try(PreparedStatement statement = con.prepareStatement(DROP_SQL)){
            statement.execute();
            System.out.println("Student table deleted");
        }
    }

    public static void viewTable(Connection con) throws SQLException{
        try(PreparedStatement statement = con.prepareStatement(VIEW_SQL)){
            ResultSet rs = statement.executeQuery();
            while (rs.next()){
                System.out.println(rs.getInt("id") + " " + rs.getString("name") + " " + rs.getString("address"));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://localhost:5432/jdbcdemo";
        String username = "postgres";
        String password = "1234";

        Connection con = DriverManager.getConnection(url, username, password);
        try {
            con.setAutoCommit(false);
            System.out.println("Connection Established");
            createTable(con);
            System.out.println("Table created");

           /* insertStudent(con, 1, "John", "Madurai");
            insertStudent(con,2,"Naveen","Tirunelveli");
            insertStudent(con,3,"Ashwin","Tenkasi");*/

            viewTable(con);
            storedFunction(con,1);
            updateFunction(con,2,"Roshan");
            updateFunction(con,3,"Joy");
            namesFunction(con);
            namesWith_inout(con,3);
            viewTable(con);
            //dropTable(con);
            con.commit();
        } catch (Exception e) {
            if (con!=null){
                try{
                    con.rollback();
                    System.err.println("Transaction rollback initiated due to error");
                } catch (SQLException rollbackException) {
                    System.err.println("Rollback failed: " + rollbackException.getMessage());
                }
            }
            throw new RuntimeException(e);
        }
        finally {
            if (con!=null){
                try {
                    con.setAutoCommit(true);
                    con.close();
                } catch (SQLException closeException) {
                    closeException.printStackTrace();
                }
            }

        }
    }


}

