import java.awt.Component;
import java.io.FileInputStream;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import javax.swing.JOptionPane;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.mongodb.*;

public class EnviarDadosBroker implements MqttCallback {

	static MqttClient mqttclient;
	static String cloud_server = new String();
	static String cloud_topic = new String();


	public static void main(String[] var0) {
		try {
			Properties var1 = new Properties();
			var1.load(new FileInputStream("SimulateSensor.ini"));
			cloud_server = var1.getProperty("cloud_server");
			cloud_topic = var1.getProperty("cloud_topic");
		} catch (Exception var2) {
			System.out.println("Error reading SimulateSensor.ini file " + var2);
			JOptionPane.showMessageDialog((Component) null, "The SimulateSensor.ini file wasn't found.",
					"Mongo To Cloud", 0);
		}

		(new EnviarDadosBroker()).connecCloud();
		(new EnviarDadosBroker()).writeSensor();
	}

	public void connecCloud() {
		try {
			mqttclient = new MqttClient(cloud_server, "SimulateSensor" + cloud_topic);
			mqttclient.connect();
			mqttclient.setCallback(this);
			mqttclient.subscribe(cloud_topic);
		} catch (MqttException var2) {
			var2.printStackTrace();
		}
	}

	public void writeSensor() {

		Double valorBefore = 60.0 ;
		
		while (true) {
			
			Random r = new Random();
			valorBefore = valorBefore + r.nextDouble() * 4;
			int valor = valorBefore.intValue();
			//DecimalFormat df = new DecimalFormat("#.#");
			//String valor = df.format(valorBefore);
			//valor = valor.replace(",", ".");
						
			String res = new String("{\"tmp\":\"19.5\",\"hum\":\"35.0\",\"dat\":\"" + LocalDate.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy"))
					+ "\",\"tim\":\"" + LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "\",\"cell\":\"" + valor + "\",\"mov\":\"1\",\"sens\":\"eth\"}");
			System.out.println(res);
			
			this.publishSensor(res);

			try {
				Thread.sleep(2000L);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void publishSensor(String var1) {
		try {
			MqttMessage var2 = new MqttMessage();
			var2.setPayload(var1.getBytes());
			mqttclient.publish(cloud_topic, var2);
		} catch (MqttException var3) {
			var3.printStackTrace();
		}

	}

	@Override
	public void connectionLost(Throwable var1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken var1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void messageArrived(String var1, MqttMessage var2) throws Exception {
		// TODO Auto-generated method stub

	}
}