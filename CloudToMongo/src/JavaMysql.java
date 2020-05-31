
import java.sql.*;
import java.time.LocalTime;
import java.util.Stack;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class JavaMysql {
	
	static Connection conn;
	static Statement s;
	static ResultSet rs;

	CloudToMongo cloudToMongo;


	BloquingQueue<MedicoesSensores> bq = new BloquingQueue<>();
	
	public BloquingQueue<MedicoesSensores> getBq() {
		return bq;
	}

//	public void getMsgsFalhadas(MedicoesSensores medicao, DBCollection coll) throws SQLException, InterruptedException {
//		String SqlCommando = new String();
//		int result = 0;
//		falhou = false;
//		System.out.println("Mysql voltou a funcionar");
//		DBCursor cursor = getMedicoesSince(coll, medicao.getTime().toString());
//		System.out.println("Medicoes falhadas: " + cursor.size());
//		while (cursor.hasNext()) {
//			DBObject obj = cursor.next();
//			MedicoesSensores med = new MedicoesSensores(new String("\"" + tipoSensor + "\""), new String("\"" + (String) obj.get(tipoSensor) + "\""), new String("\"" + (String) obj.get("dat") + "\""));
//			if (tipoSensor.contentEquals("tmp") || tipoSensor.contentEquals("hum")) {
//				bq.offer(med);
//				System.out.println("msg enviadaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
//			} else {
//				bq.offer(med);
//			}
//		}
//
//	}
	
	public void putDataIntoMysql() {
		String SqlCommando = new String();
		int result = 0;
		String database_password = new String();
		String database_user = new String();
		String database_connection = new String();
		database_password = "";
		database_user = "root";
		database_connection = "jdbc:mysql://localhost/rrr";
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			conn = DriverManager.getConnection(database_connection + "?useTimezone=true&serverTimezone=UTC", database_user, database_password);
			System.out.println("Connected");
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
 
//	private DBCursor getMedicoesSince(DBCollection coll, String dataAtual) {
//		System.out.println("Ligação falhou às: " + time.toString());
//		System.out.println("Ligação voltou às: " + dataAtual);
//		DBCursor cursor = coll.find((DBObject)JSON.parse(new String("{dat: {$gte: \"" + time.toString() + "\", $lt: \"" + dataAtual + "\"}}")));
//		return cursor;
//	}
	
	
	private void inserirNaStack(MedicoesSensores medicao, Stack<Double> last) {
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		last.push(valor);
		if (last.size() > 30) {
			last.remove(last.firstElement());
		}
		System.out.println(last);
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
