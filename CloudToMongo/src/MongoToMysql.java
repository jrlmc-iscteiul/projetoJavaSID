import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import com.mongodb.*;

public class MongoToMysql {
	
	public static Connection connectionSQL;
	public static DBCollection collection;
	
	public static void connectToMySQL() throws ClassNotFoundException, SQLException {
		String db = "mysql_main_g5";
		String user = "root";
		String pass = "";
		Class.forName("com.mysql.cj.jdbc.Driver");
		connectionSQL = DriverManager.getConnection("jdbc:mysql://localhost/" + db + "?useTimezone=true&serverTimezone=UTC", user, pass);
	}
	
	public static void connectToMongo() {
		MongoClient mongoClient1 = new MongoClient();
		DB db = mongoClient1.getDB("mysql_main_g5");
		collection = db.getCollection("sid2020");
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
		connectToMongo();
		connectToMySQL();
		/*
		DBCursor cursor = collection.find();
		while(cursor.hasNext()) {
		    System.out.println(cursor.next());
		}
		*/
		
		int id = 1;
		String tipo = "cell";
		for (;;) {
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date now = new Date();
			String data = dateFormat.format(now);
			Random r = new Random();
			double valor = r.nextDouble()*15.0 + 55.0;
		//	String query2 = "INSERT INTO MedicoesSensores VALUES ('" + id + "','" + valor + "','" + tipo + "','" + data + "','0.5'" +  ");";
			String query = new String("{\"tmp\":\"19.5\",\"hum\":\"35.0\",\"dat\":\"28/05/2020\",\"tim\":\"14:09:24\",\"cell\":\"20\",\"mov\":\"1\",\"sens\":\"eth\"}");
			System.out.println(query);
		//	int mySQLRow = connectionSQL.createStatement().executeUpdate(query);
			id++;
			Thread.sleep(5000);
		}
		
//		int id = 7;
//		double limite = 20;
//		String text = "bla";
//		String tipo = "tmp";
//		for (;;) {
//			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//			Date now = new Date();
//			String data = dateFormat.format(now);
//			Random r = new Random();
//			double valor = r.nextDouble()*5.0 + 21.0;
//			String query = "INSERT INTO Alerta VALUES ('" + id + "','" + data + "','" + tipo + "','" + valor + "','" + limite + "','" + text + "','" + 1 + "','" + text + "');";
//			System.out.println(query);
//			int mySQLRow = connectionSQL.createStatement().executeUpdate(query);
//			id++;
//			valor++;
//			Thread.sleep(3000);
//		}
		
	}

}
