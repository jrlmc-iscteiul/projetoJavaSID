
import java.sql.*;
import java.util.List;
import java.util.Stack;

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
	private Stack<Double> lastMedicoes = new Stack<Double>();
	
	
	public JavaMysql(String t) {
		tipoSensor = t;
	}
	
	public void putDataIntoMysql(MedicoesSensores medicao, DBCollection coll) {
		String SqlCommando = new String();
		int result = 0;
		String database_password = new String();
		String database_user = new String();
		String database_connection = new String();
		int maxIdCliente = 0;
		database_password = "";
		database_user = "root";
		database_connection = "jdbc:mysql://localhost/rrr";
		
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
			return;
		}
		
		try {
			if(falhou) {
				falhou = false;
				System.out.println("Mysql voltou a funcionar");
				DBCursor cursor = getMedicoesSince(coll, medicao.getTime().toString());
				int i = cursor.size();
				System.out.println("Medicoes falhadas: " + i);
				while (cursor.hasNext()) {
					DBObject obj = cursor.next();
					System.out.println(i--);
					MedicoesSensores med = new MedicoesSensores(new String("\"" + tipoSensor + "\""), new String("\"" + (String) obj.get(tipoSensor) + "\""), new String("\"" + (String) obj.get("dat") + "\""));
					if(tipoSensor.contentEquals("tmp") || tipoSensor.contentEquals("hum")) {
						Statement ss = conn.createStatement();
						inserirNaStack(med, lastMedicoes);
						coloca(SqlCommando, result, med, mediaLast(lastMedicoes), ss);
						System.out.println("msg enviadaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
						ss.close();
					}
					else {
						coloca(SqlCommando, result, med, 0, s);
					}
				}
			}
			s = conn.createStatement();
			inserirNaStack(medicao, lastMedicoes);
			coloca(SqlCommando, result, medicao, mediaLast(lastMedicoes), s);
			s.close();
			System.out.println("Passou para o MySQL");
		} catch (Exception e) {
			System.out.println("Error quering  the database . " + e);
		}
	}
	
	private static void coloca(String SqlCommando, int result, MedicoesSensores medicao, double media, Statement ss) throws SQLException {
		SqlCommando = "Insert into medicoessensores (IDMedicao, ValorMedicao, TipoSensor, DataHoraMedicao, MediaUltimasMedicoes) "
				+ "values (NULL, " + medicao.getValorMedicao() + ", " + medicao.getTipoSensor() + ", " + medicao.getData() + ", " + media + ");" ;
		result = new Integer(ss.executeUpdate(SqlCommando));
	}
 
	private DBCursor getMedicoesSince(DBCollection coll, String dataAtual) {
		System.out.println("Ligação falhou às: " + time.toString());
		System.out.println("Ligação voltou às: " + dataAtual);
		DBCursor cursor = coll.find((DBObject)JSON.parse(new String("{dat: {$gte: \"" + time.toString() + "\", $lt: \"" + dataAtual + "\"}}")));
		return cursor;
	}
	
	private double mediaLast(Stack<Double> last) {
		double sum = 0;
		for (int i = 1; i < last.size(); i++) {
			double variacao = last.get(i) - last.get(i - 1);
			sum = sum + variacao;
		}
		return sum / last.size();
	}
	
	private void inserirNaStack(MedicoesSensores medicao, Stack<Double> last) {
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		last.push(valor);
		if (last.size() > 30) {
			last.remove(last.firstElement());
		}
		System.out.println(last);
	}
	
	public Stack<Double> getLastMedicoes() {
		return lastMedicoes;
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
