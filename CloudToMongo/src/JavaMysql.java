
import java.sql.*;
import java.util.List;

public class JavaMysql {
	static Connection conn;
	static Statement s;
	static ResultSet rs;
	

	public static void putDataIntoMysql(MedicoesSensores medicao, double media) {
		String SqlCommando = new String();
		int result;
		String database_password = new String();
		String database_user = new String();
		String database_connection = new String();
	
		database_password = "";
		database_user = "root";
		database_connection = "jdbc:mysql://localhost/mysql_main_g5";
		
		try {
			
			Class.forName("com.mysql.cj.jdbc.Driver");
	
			conn = DriverManager.getConnection(database_connection + "?useTimezone=true&serverTimezone=UTC", database_user, database_password);
			System.out.println("Connected");
			
		} catch (Exception e) {
			System.out.println("Server down, unable to make the connection. ");
		}
		
		try {
			s = conn.createStatement();
		
			SqlCommando = "Insert into medicoessensores (IDMedicao, ValorMedicao, TipoSensor, DataHoraMedicao, MediaUltimasMedicoes) "
					+ "values (NULL, " + medicao.getValorMedicao() + ", " + medicao.getTipoSensor() + ", " + medicao.getData() + ", " + media + ");" ;
			
			result = new Integer(s.executeUpdate(SqlCommando));
			
			s.close();
			
		} catch (Exception e) {
			System.out.println("Error quering  the database . " + e);
		}

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
