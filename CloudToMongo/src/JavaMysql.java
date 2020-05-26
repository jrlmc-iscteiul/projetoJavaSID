
import java.sql.*;
import java.util.List;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class JavaMysql {
	
	static Connection conn;
	static Statement s;
	static ResultSet rs;
	private static boolean falhou = false;
	private static TimeStamp time;
	CloudToMongo cloudToMongo;
	private String tipoSensor;
	
	
	public JavaMysql(String t) {
		tipoSensor = t;
	}
	
	public void putDataIntoMysql(MedicoesSensores medicao, double media, DBCollection coll) {
		String SqlCommando = new String();
		int result = 0;
		String database_password = new String();
		String database_user = new String();
		String database_connection = new String();
		int maxIdCliente = 0;
		database_password = "";
		database_user = "root";
		database_connection = "jdbc:mysql://localhost/nova2";
		
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			conn = DriverManager.getConnection(database_connection + "?useTimezone=true&serverTimezone=UTC", database_user, database_password);
			System.out.println("Connected");
		} catch (Exception e) {
			if(!falhou) {
				time = medicao.getTime();
			}
			falhou = true;
			System.out.println("Server down, unable to make the connection. ");
		}
		try {
			if(falhou) {
				DBCursor cursor = getMedicoesSince(time.toMiliseconds(time.toString()), coll);
				while (cursor.hasNext()) {
					DBObject obj = cursor.next();
					MedicoesSensores med = new MedicoesSensores(tipoSensor, (String) obj.get(tipoSensor), (String) obj.get("dat"));
					coloca(SqlCommando, result, med, 0);
				}
			}
			falhou = false;
			s = conn.createStatement();
			
			coloca(SqlCommando, result, medicao, media);
			
			s.close();
			System.out.println("Passou para o MySQL");
		} catch (Exception e) {
			System.out.println("Error quering  the database . " + e);
		}
	}
	
	private static void coloca(String SqlCommando, int result, MedicoesSensores medicao, double media) throws SQLException {
		SqlCommando = "Insert into medicoessensores (IDMedicao, ValorMedicao, TipoSensor, DataHoraMedicao, MediaUltimasMedicoes) "
				+ "values (NULL, " + medicao.getValorMedicao() + ", " + medicao.getTipoSensor() + ", " + medicao.getData() + ", " + media + ");" ;
		
		result = new Integer(s.executeUpdate(SqlCommando));
	}
 
	private DBCursor getMedicoesSince(long millis, DBCollection coll) {
		DBCursor cursor = cloudToMongo.mongocolTmp.find((DBObject)JSON.parse(new String("{_id:{$gt: ObjectId(Math.floor((new Date(" + millis + "))/1000).toString(16) + \"0000000000000000\")}})")));
		return cursor;
	}
	
	public static void main(String[] args) {
//		String s = new String("{\"tmp\":\"19.30\",\"hum\":\"95.00\",\"dat\":\"19/4/2020\",\"tim\":\"9:50:51\",\"cell\":\"228\"\"mov\":\"0\"\"mov\":\"1\",\"sens\":\"eth\"}");
//		List<MedicoesSensores> ms = MedicoesSensores.criarMedicao(s);
//		
//		for (MedicoesSensores medicao : ms) {
//			putDataIntoMysql(medicao);
//		}
	}
}
