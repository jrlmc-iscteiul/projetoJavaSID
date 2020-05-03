import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Stack;

import javax.swing.JOptionPane;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.mongodb.*;
import com.mongodb.util.JSON;

public class CloudToMongo implements MqttCallback {

	MqttClient mqttclient;
	static MongoClient mongoClient;
	static DB db;
	
	static DBCollection mongocolTmp;
	static DBCollection mongocolHum;
	static DBCollection mongocolLum;
	static DBCollection mongocolMov;
	static DBCollection mongocolLixo;
	
	static String cloud_server = new String();
	static String cloud_topic = new String();
	static String mongo_host = new String();
	static String mongo_database = new String();
	
	static String mongo_collection_temperatura = new String();
	static String mongo_collection_humidade = new String();
	static String mongo_collection_luminosidade = new String();
	static String mongo_collection_movimento = new String();
	static String mongo_collection_msgDescartadas = new String();
	
	private Stack<Double> lastHumidades;
	private Stack<Double> lastTemperaturas;

	public static void main(String[] args) {

		try {
			Properties p = new Properties();
			p.load(new FileInputStream("cloudToMongo.ini"));
			
			cloud_server = p.getProperty("cloud_server");
			cloud_topic = p.getProperty("cloud_topic");
			mongo_host = p.getProperty("mongo_host");
			mongo_database = p.getProperty("mongo_database");
			
			mongo_collection_temperatura = p.getProperty("mongo_collection_temperatura");
			mongo_collection_humidade = p.getProperty("mongo_collection_humidade");
			mongo_collection_luminosidade = p.getProperty("mongo_collection_luminosidade");
			mongo_collection_movimento = p.getProperty("mongo_collection_movimento");
			mongo_collection_msgDescartadas = p.getProperty("mongo_collection_msgDescartadas");
			
		} catch (Exception e) {

			System.out.println("Error reading CloudToMongo.ini file " + e);
			JOptionPane.showMessageDialog(null, "The CloudToMongo.ini file wasn't found.", "CloudToMongo",
					JOptionPane.ERROR_MESSAGE);
		}
		new CloudToMongo().connecCloud();
		new CloudToMongo().connectMongo();
	}

	public void connecCloud() {
		int i;
		try {
			i = new Random().nextInt(100000);
			mqttclient = new MqttClient(cloud_server, "CloudToMongo_" + String.valueOf(i) + "_" + cloud_topic);
			mqttclient.connect();
			mqttclient.setCallback(this);
			mqttclient.subscribe(cloud_topic);
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

	public void connectMongo() {
		mongoClient = new com.mongodb.MongoClient();
		mongoClient = new MongoClient(new MongoClientURI(mongo_host));
		db = mongoClient.getDB(mongo_database);
		mongocolTmp = db.getCollection(mongo_collection_temperatura);
		mongocolHum = db.getCollection(mongo_collection_humidade);
		mongocolLum = db.getCollection(mongo_collection_luminosidade);
		mongocolMov = db.getCollection(mongo_collection_movimento);
		mongocolLixo = db.getCollection(mongo_collection_msgDescartadas);
	}

	public double mediaLast(MedicoesSensores medicao, Stack<Double> last) {
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		last.push(valor);
		if (last.size() > 30) {
			last.remove(last.firstElement());
		}
		System.out.println(last);
		double sum = 0;
		for (int i = 1; i < last.size(); i++) {
			double variacao = last.get(i) - last.get(i - 1);
			sum = sum + variacao;
		}
		return sum / last.size();
	}

	public void filtrarTemperatura(MedicoesSensores medicao) {
	
		if(mediaLast(medicao, lastTemperaturas) > 0.4 || mediaLast(medicao, lastTemperaturas) < -0.4) {
			mongocolLixo.insert((DBObject) JSON.parse(clean(medicao.toString())));
			System.out.println("lixo");
		} else {
			mongocolTmp.insert((DBObject) JSON.parse(clean(medicao.toString())));
			System.out.println("e");
		}
	}
	
	public void filtrarHumidade(MedicoesSensores medicao) {
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		if(valor > 100 || valor < 0 || mediaLast(medicao, lastHumidades) > 0.4 || mediaLast(medicao, lastHumidades) < -0.4) {
			mongocolLixo.insert((DBObject) JSON.parse(clean(medicao.toString())));
			System.out.println("lixo");
		} else {
			mongocolHum.insert((DBObject) JSON.parse(clean(medicao.toString())));
			System.out.println("e");
		}
	}
	
	public void filtrarMovimento() {
		
	}
	
	public void filtrarLuminosidade() {
		
	}
	
	@Override
	public void messageArrived(String topic, MqttMessage c) throws Exception {
		try {
			System.out.println(c.toString());

			List<MedicoesSensores> medicoes = MedicoesSensores.criarMedicao(c.toString());

			for (MedicoesSensores medicao : medicoes) {

				if (medicao.getTipoSensor().equals("\"tmp\"")) {
					filtrarTemperatura(medicao);
					JavaMysql.putDataIntoMysql(medicao, mediaLast(medicao, lastTemperaturas));
				}

				if (medicao.getTipoSensor().equals("\"hum\"")) {
					filtrarHumidade(medicao);
					JavaMysql.putDataIntoMysql(medicao, mediaLast(medicao, lastHumidades));
				}

				if (medicao.getTipoSensor().equals("\"cell\"")) {
					mongocolLum.insert((DBObject) JSON.parse(clean(medicao.toString())));
					//JavaMysql.putDataIntoMysql(medicao);
				}

				if (medicao.getTipoSensor().contentEquals("\"mov\"")) {
					mongocolMov.insert((DBObject) JSON.parse(clean(medicao.toString())));
					//JavaMysql.putDataIntoMysql(medicao);
				}
			}

		} catch (Exception e) {
			System.out.println(e);
		}
	}

	@Override
	public void connectionLost(Throwable cause) {
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
	}

	public String clean(String message) {
		return (message.replaceAll("\"\"", "\","));

	}

}