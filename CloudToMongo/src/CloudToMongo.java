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

	private int lastSegundo;

	FiltrarMensagens filtrarMensagens = new FiltrarMensagens(this);

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


	public boolean verificaDuplicados(MedicoesSensores medicao) {
		if(medicao.getTime().getSegundo() != lastSegundo) {
			return true;
		}
		lastSegundo = medicao.getTime().getSegundo();
		return false;
	}

	@Override
	public void messageArrived(String topic, MqttMessage c) throws Exception {
		try {
			System.out.println("\n" + c.toString());

			List<MedicoesSensores> medicoes = MedicoesSensores.criarMedicao(c.toString());
			
			for (MedicoesSensores medicao : medicoes) {
				if (verificaDuplicados(medicao)) {
					if (medicao.getTipoSensor().equals("\"tmp\"")) {
						filtrarMensagens.filtrarTemperatura(medicao);
					}

					if (medicao.getTipoSensor().equals("\"hum\"")) {
//						filtrarMensagens.filtrarHumidade(medicao);
					}

					if (medicao.getTipoSensor().equals("\"cell\"")) {
//						filtrarMensagens.luminosidade(medicao);
					}

					if (medicao.getTipoSensor().contentEquals("\"mov\"")) {
//						filtrarMensagens.movimento(medicao);
					}
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