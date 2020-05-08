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
	
	private int ultimoSeg;
	
	//private Stack<Double> lastHumidades = new Stack<Double>();
	//private Stack<Double> lastTemperaturas = new Stack<Double>();
	
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

	private double mediaLast(Stack<Double> last) {
		double sum = 0;
		for (int i = 1; i < last.size(); i++) {
			double variacao = last.get(i) - last.get(i - 1);
			sum = sum + variacao;
		}
		return sum / last.size();
	}

	/*
	private void inserirNaStack(MedicoesSensores medicao, Stack<Double> last) {
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		last.push(valor);
		if (last.size() > 12) {
			last.remove(last.firstElement());
		}
		System.out.println(last);
	}
	
	public void filtrarTemperatura(MedicoesSensores medicao) {
		inserirNaStack(medicao, lastHumidades);
		List<Double> limites = outliers(lastTemperaturas);
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		if(valor < limites.get(0) || valor > limites.get(1)) {
			mongocolLixo.insert((DBObject) JSON.parse(clean(medicao.toString())));
			System.out.println("lixo");
		} else {
			mongocolTmp.insert((DBObject) JSON.parse(clean(medicao.toString())));
			System.out.println("bom");
		}
	}
	
	public void filtrarHumidade(MedicoesSensores medicao) {
//		String v = medicao.getValorMedicao();
//		double valor = Double.parseDouble(v.replace("\"", ""));
//		if(valor > 100 || valor < 0 || mediaLast(medicao, lastHumidades) > 0.4 || mediaLast(medicao, lastHumidades) < -0.4) {
//			mongocolLixo.insert((DBObject) JSON.parse(clean(medicao.toString())));
//			System.out.println("lixo");
//		} else {
//			mongocolHum.insert((DBObject) JSON.parse(clean(medicao.toString())));
//			System.out.println("e");
//		}
	}
	
	public void filtrarMovimento() {
		
	}
	
	public void filtrarLuminosidade() {
		
	}
	
	private List<Double> outliers(Stack<Double> last) {
		Stack<Double> copy = new Stack<Double>();
		copy.addAll(last);
		Stack<Double> stackOrdenada = ordenarStack(copy);
		List<Double> limites = new ArrayList<>();
		double q1 = (stackOrdenada.elementAt(8) + stackOrdenada.elementAt(9))/2;
		double q3 = (stackOrdenada.elementAt(2) + stackOrdenada.elementAt(3))/2;
		double aiq = q3 - q1;
		if(between(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(9),0,2)) {
			limites.add((q1-aiq*20)+2);
			limites.add((q3+aiq*20)+2);
		} else if(between(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(9),2,5)) {
			limites.add(q1-aiq*6);
			limites.add(q3+aiq*6);
		} else {
			limites.add(q1-aiq*4);
			limites.add(q3+aiq*4);
		}
		System.out.println(limites);
		return limites;
	}
	
	private boolean between(double d, int min, int max) {
	    return (d >= min && d < max);
	}
	
	private Stack<Double> ordenarStack(Stack<Double> in) {
		Stack<Double> stackOrdenada = new Stack<>();
		while(!in.isEmpty()) { 
            double tmp = in.pop(); 
            while(!stackOrdenada.isEmpty() && stackOrdenada.peek() < tmp)  { 
            	in.push(stackOrdenada.pop()); 
            } 
            stackOrdenada.push(tmp); 
        }
		return stackOrdenada;
	} */
	
	public boolean verificaDuplicados(MedicoesSensores medicao) {
		String[] tokens = medicao.getData().split(":");
		tokens[tokens.length-1] = tokens[tokens.length-1].replace("\"", "");
		int seg = Integer.parseInt(tokens[tokens.length-1]);
		if(seg != ultimoSeg) {
			return true;
		}
		ultimoSeg = seg;
		return false;
	}
	
	@Override
	public void messageArrived(String topic, MqttMessage c) throws Exception {
		try {
			System.out.println(c.toString());

			List<MedicoesSensores> medicoes = MedicoesSensores.criarMedicao(c.toString());

			for (MedicoesSensores medicao : medicoes) {
				if(verificaDuplicados(medicao)) {
				if (medicao.getTipoSensor().equals("\"tmp\"")) {
					filtrarMensagens.filtrarTemperatura(medicao);
					JavaMysql.putDataIntoMysql(medicao, mediaLast(filtrarMensagens.getLastTemperaturas()));
				}

				if (medicao.getTipoSensor().equals("\"hum\"")) {
					filtrarMensagens.filtrarHumidade(medicao);
					JavaMysql.putDataIntoMysql(medicao, mediaLast(filtrarMensagens.getLastHumidades()));
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