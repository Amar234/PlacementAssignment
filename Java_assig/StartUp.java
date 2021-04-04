import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import au.com.bytecode.opencsv.*;
import breeze.linalg.reverse;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.analysis.function.Inverse;
public class StartUp {

	public StartUp() {
	}

	//public static void main(String[] args) throws ClassNotFoundException, SQLException {
		//System.out.println("Hello HsqlDB !!");
	/*	Connection con = null;
        try
        {
            Class.forName("org.hsqldb.jdbcDriver");         
            con = DriverManager.getConnection("jdbc:hsqldb:file:C:\\hsqldb-2.6.0\\hsqldb\\hql", "SA","SA");    

            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM EMP");
            System.out.println("Connection is established successfully...");

            while(rs.next())
            {
                System.out.println(rs.getString(1) +" "+ rs.getString(2));
            }
            con.close();

        }
        catch(Exception ex )
        {
            ex.printStackTrace();
        }
        finally
        {
            if(con!=null)
            {
                 con.close();
                 System.out.println("Connection closed !!");
            }
        }
    }
}*/

    public static void main(String[] args) {
        String jdbcURL = "jdbc:hsqldb:file:C:\\hsqldb-2.6.0\\hsqldb\\hql";
        String username = "SA";
        String password = "SA";
 
        String csvFilePath = "D:\\PROJECTS\\Eclipse New Workspace\\Dataease_Assignment\\startup.csv";
 
        int batchSize = 20;
 
        Connection connection = null;
 
        try {
 
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);
            
            String sql = "INSERT INTO STARTUP (sr_no,date,startup_name,industry_vertical,Subvertical,city,Investors_Name,InvestmentnType,Amount_in_USD,remarks) VALUES (?,TO_DATE( ?, 'yyyy/dd/mm'), ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);
            //statement.setDate(2, "dd/mm/yyyy");
            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;
 
            int count = 0;
 
            lineReader.readLine(); // skip header line
 
            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
                String sr_no = data[0];
                String date = data[1];
                String startup_name = data[2];
                String industry_vertical = data[3];
                String Subvertical = data[4];
                String city = data[5];
                String Investors_Name= data[6];
                String InvestmentnType =data[7];
                String Amount_in_USD = data[8];
                String remarks = data[9];
               
                statement.setString(1, sr_no);
                statement.setString(2, date);
 
                Timestamp sqlTimestamp = Timestamp.valueOf(date);
                statement.setTimestamp(2, sqlTimestamp);
                statement.setString(3, startup_name);
                statement.setString(4, industry_vertical);
                statement.setString(5, Subvertical);
                statement.setString(6,city);
                statement.setString(7, Investors_Name);
                statement.setString(8, InvestmentnType);
                statement.setString(9, Amount_in_USD);
                statement.setString(10, remarks);

                statement.addBatch();
 
                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            }
 
            lineReader.close();
 
            // execute the remaining queries
            statement.executeBatch();
 
            connection.commit();
            connection.close();
 
        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();
 
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
 
    }
}