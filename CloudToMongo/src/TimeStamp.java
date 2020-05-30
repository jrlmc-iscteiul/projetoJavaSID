import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeStamp {

	private int ano;
	private int mes;
	private int dia;
	private int hora;
	private int minuto;
	private int segundo;
	
	public TimeStamp(int ano, int mes, int dia, int hora, int minuto, int segundo) {
		super();
		this.ano = ano;
		this.mes = mes;
		this.dia = dia;
		this.hora = hora;
		this.minuto = minuto;
		this.segundo = segundo;
	}
	
	public long toMiliseconds(String myDate) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = sdf.parse(myDate);
		long millis = date.getTime();
		return millis;
	}

	public int getAno() {
		return ano;
	}

	public void setAno(int ano) {
		this.ano = ano;
	}

	public int getMes() {
		return mes;
	}

	public void setMes(int mes) {
		this.mes = mes;
	}

	public int getDia() {
		return dia;
	}

	public void setDia(int dia) {
		this.dia = dia;
	}

	public int getHora() {
		return hora;
	}

	public void setHora(int hora) {
		this.hora = hora;
	}

	public int getMinuto() {
		return minuto;
	}

	public void setMinuto(int minuto) {
		this.minuto = minuto;
	}

	public int getSegundo() {
		return segundo;
	}

	public void setSegundo(int segundo) {
		this.segundo = segundo;
	}

	@Override
	public String toString() {
		return (ano + "-" + String.format("%02d", mes) + "-" + String.format("%02d", dia) + " " + String.format("%02d", hora) + ":" + String.format("%02d", minuto) + ":" + String.format("%02d", segundo));
	}
	
	public static void main(String[] args) throws ParseException {
		TimeStamp t = new TimeStamp(2020, 5, 29, 22, 5, 31);
		System.out.println(t.toString());
		System.out.println(t.toMiliseconds(t.toString()));
	}	
}
