
import java.sql.*;
import java.time.LocalTime;

public class JavaMysql {
	
	static Connection conn;
	static Statement s;
	static ResultSet rs;

	CloudToMongo cloudToMongo;

	BlockingQueue<MedicoesSensores> bq = new BlockingQueue<>();
	
	public BlockingQueue<MedicoesSensores> getBq() {
		return bq;
	}
	
	public void putDataIntoMysql() {
		String SqlCommando = new String();
		int result = 0;
		String database_password = new String();
		String database_user = new String();
		String database_connection = new String();
		database_password = "";
		database_user = "root";
		database_connection = "jdbc:mysql://localhost/mysql_main_g5";
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			conn = DriverManager.getConnection(database_connection + "?useTimezone=true&serverTimezone=UTC", database_user, database_password);
			System.out.println("Connected to mysql");
		} catch (Exception e) {
			System.out.println("Server down, unable to make the connection. ");
		}
		
		try {
			s = conn.createStatement();
			coloca(SqlCommando, result, bq.take(), s);
			s.close();
		} catch(Exception e) {
			LocalTime lt = LocalTime.now();
			System.out.println(lt);
		}

	}

	private static void coloca(String SqlCommando, int result, MedicoesSensores medicao, Statement ss) throws SQLException {
		SqlCommando = "Insert into medicoessensores (IDMedicao, ValorMedicao, TipoSensor, DataHoraMedicao, MediaUltimasMedicoes) "
				+ "values (NULL, " + medicao.getValorMedicao() + ", " + medicao.getTipoSensor() + ", " + medicao.getData() + ", " + medicao.getMedia() + ");" ;
		result = new Integer(ss.executeUpdate(SqlCommando));
		System.out.println("Mensagem inserida: " + medicao);
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
