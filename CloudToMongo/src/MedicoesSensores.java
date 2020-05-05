import java.util.ArrayList;
import java.util.List;

public class MedicoesSensores {
	
	 private String tipoSensor;
	 private String valorMedicao;
	 private String data;
	
	 public MedicoesSensores(String tipoSensor, String valorMedicao, String data) {
		super();
		this.tipoSensor = tipoSensor;
		this.valorMedicao = valorMedicao;
		this.data = data;
	}
	 
	
	 public String getTipoSensor() {
		return tipoSensor;
	}

	public String getValorMedicao() {
		return valorMedicao;
	}

	public String getData() {
		return data;
	}

	
	public static List<MedicoesSensores> criarMedicao (String medicao) {
		
		if(medicao.contains("\"mov\":\"0\""))
			medicao = medicao.replace("\"mov\":\"0\"", ",");
		 
		System.out.println("medicao: " +  medicao);
		
		String[] parts = medicao.split(",");
		System.out.println("medicao 1");
		String[] partsTemp = parts[0].split(":");
		String sensorTemp = partsTemp[0].replace("{", "");
		System.out.println("medicao 2");
		String[] partsHum = parts[1].split(":");
		System.out.println("medicao 3");
		String data[] = parts[2].split(":");
		System.out.println("medicao 4");
		String[] horaParts = parts[3].split(":");
		String hora = new String(horaParts[1] + ":" + horaParts[2] + ":" + horaParts[3]);
		System.out.println("medicao 5: " + parts[4]);
		String[] partsLum = parts[4].split(":");
		System.out.println("medicao 6: " + partsLum);
		//String[] partsLum = parts[2].split(":");
		//partsMov[1] = partsMov[1].replace("\"sens\"", "");
		System.out.println("medicao 7");
		MedicoesSensores msTemp = new MedicoesSensores(sensorTemp, partsTemp[1], criarTimestamp(data[1], hora));
		MedicoesSensores msHum = new MedicoesSensores(partsHum[0], partsHum[1], criarTimestamp(data[1], hora));
		MedicoesSensores msLum = new MedicoesSensores(partsLum[0], partsLum[1], criarTimestamp(data[1], hora));
//		MedicoesSensores msMov = new MedicoesSensores(partsMov[0], partsMov[1], criarTimestamp(data[1], hora));
		System.out.println("medicao 8");
		List<MedicoesSensores> medicoes = new ArrayList<MedicoesSensores>();
		System.out.println("medicao 9");
		medicoes.add(msTemp);
		medicoes.add(msHum);
		medicoes.add(msLum);
		//medicoes.add(msMov);
		
		System.out.println(medicoes.toString());
		return medicoes;
	 }
	 
	 private static String criarTimestamp(String data, String hora) {
		 data = data.replace("\"", "");
		 hora = hora.replace("\"", "");
		 String[] dataParts = data.split("/");
		 String timestamp = new String("\"" + dataParts[2] + "-" + dataParts[1] + "-" + dataParts[0] + " " + hora + "\"");
		 return timestamp;
	 }
	 
	 public static Double tirarAspasValorMedicao (MedicoesSensores medicao) {
		return Double.parseDouble(medicao.getValorMedicao().replace("\"", ""));
	 }
	 
	 @Override
	public String toString() {
		 StringBuilder sb = new StringBuilder();
		 sb.append("{" + tipoSensor + ":" + valorMedicao + ",\"dat\":" + data + "}");
		 
		 return sb.toString();
	}
	 
	 
	 public static void main(String[] args) {
		 
		 List<MedicoesSensores> ms = criarMedicao("{\"tmp\":\"19.30\",\"hum\":\"95.00\",\"dat\":\"19/4/2020\",\"tim\":\"9:50:51\",\"cell\":\"228\"\"mov\":\"0\"\"mov\":\"1\",\"sens\":\"eth\"}");
		 System.out.println(ms.toString());
		 
		 System.out.println(MedicoesSensores.tirarAspasValorMedicao(ms.get(0)));
	
	}
}
