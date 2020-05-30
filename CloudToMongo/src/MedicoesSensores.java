import java.util.ArrayList;
import java.util.List;

public class MedicoesSensores {

	private String tipoSensor;
	private String valorMedicao;
	private String data;
	private static TimeStamp time;

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

	public static List<MedicoesSensores> criarMedicao(String medicao) {

		if (medicao.contains("\"mov\":\"0\"\"mov\":\"1\""))
			medicao = medicao.replace("\"mov\":\"0\"", ",");

		String[] parts = medicao.split(",");

		String[] partsTemp = parts[0].split(":");
		String sensorTemp = partsTemp[0].replace("{", "");

		String[] partsHum = parts[1].split(":");

		String data[] = parts[2].split(":");

		String[] horaParts = parts[3].split(":");
		String hora = new String(horaParts[1] + ":" + horaParts[2] + ":" + horaParts[3]);

		String[] partsLum = parts[4].split(":");

		String[] partsMov = parts[5].split(":");
		partsMov[1] = partsMov[1].replace("\"sens\"", "");

		MedicoesSensores msTemp = new MedicoesSensores(sensorTemp, partsTemp[1], criarTimestamp(data[1], hora));
		MedicoesSensores msHum = new MedicoesSensores(partsHum[0], partsHum[1], criarTimestamp(data[1], hora));
		MedicoesSensores msLum = new MedicoesSensores(partsLum[0], partsLum[1], criarTimestamp(data[1], hora));
		MedicoesSensores msMov = new MedicoesSensores(partsMov[0], partsMov[1], criarTimestamp(data[1], hora));

		List<MedicoesSensores> medicoes = new ArrayList<MedicoesSensores>();

		medicoes.add(msTemp);
		medicoes.add(msHum);
		medicoes.add(msLum);
		medicoes.add(msMov);

		return medicoes;
	}

	private static String criarTimestamp(String data, String hora) {
		data = data.replace("\"", "");
		hora = hora.replace("\"", "");
		String[] dat = data.split("/");
		String[] hor = hora.split(":");
		time = new TimeStamp(Integer.parseInt(dat[2]), Integer.parseInt(dat[1]), Integer.parseInt(dat[0]), Integer.parseInt(hor[0]), Integer.parseInt(hor[1]), Integer.parseInt(hor[2]));
		String[] dataParts = data.split("/");
		String timestamp = new String("\"" + dataParts[2] + "-" + dataParts[1] + "-" + dataParts[0] + " " + hora + "\"");
		return timestamp;
	}

	public static Double tirarAspasValorMedicao(MedicoesSensores medicao) {
		return Double.parseDouble(medicao.getValorMedicao().replace("\"", ""));
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{" + tipoSensor + ":" + valorMedicao + ",\"dat\":" + data + "}");

		return sb.toString();
	}

	public static void main(String[] args) {
//		List<MedicoesSensores> ms = criarMedicao("{\"tmp\":\"19.30\",\"hum\":\"95.00\",\"dat\":\"19/4/2020\",\"tim\":\"9:50:51\",\"cell\":\"228\"\"mov\":\"0\"\"mov\":\"1\",\"sens\":\"eth\"}");
//		System.out.println(ms.toString());

	}

	public TimeStamp getTime() {
		return time;
	}
}
