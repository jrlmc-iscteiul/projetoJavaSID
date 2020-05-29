import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class FiltrarMensagens {

	CloudToMongo cloudToMongo;

	private Stack<Double> lastHumidades = new Stack<Double>();
	private Stack<Double> lastTemperaturas = new Stack<Double>();
	private Stack<Double> medicoesLuminosidadeAnteriores = new Stack<Double>();
	
	private MedicoesSensores medLuminosidadeLixo = null;
	private MedicoesSensores medicaoMovAnterior = null;
	private MedicoesSensores medicaoMovLixo = null;

	private boolean haLuminosidade;
	private boolean haMovimento;
	
	private String datetime;
	
	private JavaMysql mysqlTmp = new JavaMysql("tmp");
	private JavaMysql mysqlHum = new JavaMysql("hum");
	private JavaMysql mysqlMov = new JavaMysql("mov");
	private JavaMysql mysqlLum = new JavaMysql("lum");

	public FiltrarMensagens(CloudToMongo cloudToMongo) {
		this.cloudToMongo = cloudToMongo;
	}

	public Stack<Double> getLastHumidades() {
		return lastHumidades;
	}

	public void setLastHumidades(Stack<Double> lastHumidades) {
		this.lastHumidades = lastHumidades;
	}

	public Stack<Double> getLastTemperaturas() {
		return lastTemperaturas;
	}

	public void setLastTemperaturas(Stack<Double> lastTemperaturas) {
		this.lastTemperaturas = lastTemperaturas;
	}

	private void inserirNaStack(MedicoesSensores medicao, Stack<Double> last) {
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		last.push(valor);
		if (last.size() > 30) {
			last.remove(last.firstElement());
		}
	}

	public void filtrarTemperatura(MedicoesSensores medicao) {
		if(mysqlTmp.getLastMedicoes().size() < 1) {
			inserirNaStack(medicao, mysqlTmp.getLastMedicoes());
			
		}
		List<Double> limites = outliers(mysqlTmp.getLastMedicoes(), mysqlTmp.getLastMedicoes().size());
		String v = medicao.getValorMedicao();
		System.out.println("Valor que chegou: " + v);
		double valor = Double.parseDouble(v.replace("\"", ""));
		if(valor < limites.get(0) || valor > limites.get(1)) {
			cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			System.out.println("lixo");
		} else {
			cloudToMongo.mongocolTmp.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			mysqlTmp.putDataIntoMysql(medicao, cloudToMongo.mongocolTmp);
			System.out.println("Temp foi aceite");
			System.out.println("Last temperaturas atualizado : " + mysqlTmp.getLastMedicoes());
		}
	}
	
	private double mediaLast(Stack<Double> last) {
		double sum = 0;
		for (int i = 1; i < last.size(); i++) {
			double variacao = last.get(i) - last.get(i - 1);
			sum = sum + variacao;
		}
		return sum / last.size();
	}

	public void filtrarHumidade(MedicoesSensores medicao) {
		if(lastHumidades.size() < 1) {
			inserirNaStack(medicao, lastHumidades);
		}
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		List<Double> limites = outliers(lastHumidades, lastHumidades.size());
		if(valor < limites.get(0) || valor > limites.get(1) || valor < 0 || valor > 100) {
			cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			System.out.println("lixo");
		} else {
			cloudToMongo.mongocolHum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			inserirNaStack(medicao, lastHumidades);
			mysqlHum.putDataIntoMysql(medicao, cloudToMongo.mongocolHum);
			System.out.println("Hum foi aceite");
			System.out.println("Last humidades atualizado : " + lastHumidades);
		}
	}

	public void movimento(MedicoesSensores medicaoMovAtual) {
		System.out.println("entrou movimento");
		Double valorMedicaoMovAtual = MedicoesSensores.tirarAspasValorMedicao(medicaoMovAtual);
		Double valorMedicaoMovAnterior;
		if (medicaoMovAnterior != null) {
			valorMedicaoMovAnterior = MedicoesSensores.tirarAspasValorMedicao(medicaoMovAnterior);
			if ((valorMedicaoMovAnterior == 0 && valorMedicaoMovAtual == 0)
					|| (valorMedicaoMovAnterior == 1 && valorMedicaoMovAtual == 0)
					|| (valorMedicaoMovAnterior == 1 && valorMedicaoMovAtual == 1)) {
				haMovimento = false;
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
//				JavaMysql.putDataIntoMysql(medicaoMovAtual, 0); // mudar o double

				medicaoMovAnterior = medicaoMovAtual;
			}
			if (valorMedicaoMovAnterior == 0 && valorMedicaoMovAtual == 1 && !haMovimento) {
				haMovimento = true;
				cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				medicaoMovLixo = medicaoMovAtual;
			} else if (valorMedicaoMovAnterior == 0 && valorMedicaoMovAtual == 1 && haMovimento) {
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovLixo.toString())));
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				mysqlMov.putDataIntoMysql(medicaoMovLixo, cloudToMongo.mongocolMov); // mudar valor media
				mysqlMov.putDataIntoMysql(medicaoMovAtual, cloudToMongo.mongocolMov); // mudar valor media
				cloudToMongo.mongocolLixo.findAndRemove((DBObject) JSON.parse(new String("{$and: [{dat:" + medicaoMovLixo.getData() + "}, {mov:" + medicaoMovLixo.getValorMedicao() + "}]}")));
				medicaoMovAnterior = medicaoMovAtual;
			}
		} else { // movimentoAnterior == null
			cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
			mysqlMov.putDataIntoMysql(medicaoMovAtual, cloudToMongo.mongocolMov); // mudar valor media
			medicaoMovAnterior = medicaoMovAtual;
		}
	}

	public void luminosidade(MedicoesSensores medicaoAtual) {
		System.out.println("entrou luminosidade");

		Double valorMedLuminosidadeLixo = null;
		if (medLuminosidadeLixo != null)
			valorMedLuminosidadeLixo = MedicoesSensores.tirarAspasValorMedicao(medLuminosidadeLixo);

		Double valorMedicaoAtual = MedicoesSensores.tirarAspasValorMedicao(medicaoAtual);

		if (medicoesLuminosidadeAnteriores.size() == 2) {
			
			System.out.println("mediçoes anteriores: " + medicoesLuminosidadeAnteriores.toString());

			if (((medicoesLuminosidadeAnteriores.get(1) - medicoesLuminosidadeAnteriores.get(0)) <= Math.abs(10) && (valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) <= Math.abs(10))
					|| ((medicoesLuminosidadeAnteriores.get(1) - medicoesLuminosidadeAnteriores.get(0)) <= Math.abs(10) && (valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) <= Math.abs(50))
					|| ((medicoesLuminosidadeAnteriores.get(1) - medicoesLuminosidadeAnteriores.get(0)) <= Math.abs(50) && (valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) <= Math.abs(10))
					|| ((medicoesLuminosidadeAnteriores.get(1) - medicoesLuminosidadeAnteriores.get(0)) <= Math.abs(50) && (valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) <= Math.abs(50)) ) {
				
				System.out.println("1º if");

				cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				mysqlLum.putDataIntoMysql(medicaoAtual, cloudToMongo.mongocolLum); // mudar valor media

				atualizarStackLuminosidade(medicaoAtual);
				haLuminosidade = false;
			}

			if ((medicoesLuminosidadeAnteriores.get(1) - valorMedicaoAtual) <= Math.abs(10)
					&& ((valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) > Math.abs(50)
							&& !haLuminosidade)) {
				System.out.println("2º if");
				
				haLuminosidade = true;
				medLuminosidadeLixo = medicaoAtual;

				cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				mysqlLum.putDataIntoMysql(medicaoAtual, cloudToMongo.mongocolLum); // mudar valor media

			} else if (haLuminosidade && ((valorMedLuminosidadeLixo - 10) <= valorMedicaoAtual)) {
				
				System.out.println("3º if");
				
				cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				cloudToMongo.mongocolLixo.findAndRemove((DBObject) JSON.parse(new String("{$and: [{dat:" + medLuminosidadeLixo.getData() + "}, {mov:" + medLuminosidadeLixo.getValorMedicao() + "}]}")));

				mysqlLum.putDataIntoMysql(medLuminosidadeLixo, cloudToMongo.mongocolLum); // mudar valor media
				mysqlLum.putDataIntoMysql(medicaoAtual, cloudToMongo.mongocolLum); // mudar valor media

				atualizarStackLuminosidade(medLuminosidadeLixo);
				atualizarStackLuminosidade(medicaoAtual);

				haLuminosidade = false;
			}
		} else { 
			
			cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
			mysqlLum.putDataIntoMysql(medicaoAtual, cloudToMongo.mongocolLum); //mudar valor media
			atualizarStackLuminosidade(medicaoAtual);
		}
	}

	public List<Double> outliers(Stack<Double> last, int size) {
		Stack<Double> copy = new Stack<Double>();
		copy.addAll(last);
		Stack<Double> stackOrdenada = ordenarStack(copy);
		List<Double> limites = new ArrayList<>();
		int mid1 = size/2;
		int mid2 = (size / 2) - 1;
		double q1 = 0;
		double q3 = 0;
		if(size < 8) {
			limites.add(last.lastElement()-3);
			limites.add(last.lastElement()+3);
			System.out.println("Limites: " + limites);
			return limites;
		}
		if (size % 2 == 0) {
			q1 = (stackOrdenada.elementAt(mid1 + mid1 / 2) + stackOrdenada.elementAt(mid1 + (mid1 / 2) + 1)) / 2;
			q3 = (stackOrdenada.elementAt(mid2 - (mid2 / 2) - 1) + stackOrdenada.elementAt(mid2 - (mid2 / 2) - 2)) / 2;
		} else {
			q1 = (stackOrdenada.elementAt(mid1 + mid1 / 2));
			q3 = (stackOrdenada.elementAt(mid1 - mid1 / 2));
		}
		double aiq = q3 - q1;
		System.out.println(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(size-2));
		if(between(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(size-2),0,2)) {
			limites.add((q1-aiq*7)-2);
			limites.add((q3+aiq*7)+2);
		} else if(between(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(size-2),2,5)) {
			limites.add(q1-aiq*5);
			limites.add(q3+aiq*5);
		} else {
			limites.add(q1-aiq*3);
			limites.add(q3+aiq*3);
		}
		System.out.println("Limites: " + limites);
		return limites;
	}

	private boolean between(double d, int min, int max) {
		return (d >= min && d < max);
	}

	private Stack<Double> ordenarStack(Stack<Double> in) {
		Stack<Double> stackOrdenada = new Stack<>();
		while (!in.isEmpty()) {
			double tmp = in.pop();
			while (!stackOrdenada.isEmpty() && stackOrdenada.peek() < tmp) {
				in.push(stackOrdenada.pop());
			}
			stackOrdenada.push(tmp);
		}
		return stackOrdenada;
	}

	private void atualizarStackLuminosidade(MedicoesSensores medicao) {
		if (medicoesLuminosidadeAnteriores.size() == 2) {
			medicoesLuminosidadeAnteriores.remove(0);
			Double valorMedicao = MedicoesSensores.tirarAspasValorMedicao(medicao);
			medicoesLuminosidadeAnteriores.add(valorMedicao);
		} else {
			Double valorMedicao = MedicoesSensores.tirarAspasValorMedicao(medicao);
			medicoesLuminosidadeAnteriores.add(valorMedicao);
		}
	}

	public static void main(String[] args) {

		CloudToMongo ctm = new CloudToMongo();
		FiltrarMensagens fm = new FiltrarMensagens(ctm);

		MedicoesSensores ms1 = new MedicoesSensores("\"tmp\"", "\"24.00\"", "\"2020-5-6 9:21:52\"");
		fm.filtrarTemperatura(ms1);
		MedicoesSensores ms2 = new MedicoesSensores("\"tmp\"", "\"24.00\"", "\"2020-5-6 9:21:54\"");
		fm.filtrarTemperatura(ms2);
		MedicoesSensores ms3 = new MedicoesSensores("\"tmp\"", "\"24.30\"", "\"2020-5-6 9:21:56\"");
		fm.filtrarTemperatura(ms3);
		MedicoesSensores ms4 = new MedicoesSensores("\"tmp\"", "\"24.50\"", "\"2020-5-6 9:21:58\"");
		fm.filtrarTemperatura(ms4);
		MedicoesSensores ms5 = new MedicoesSensores("\"tmp\"", "\"24.50\"", "\"2020-5-6 9:22:00\"");
		fm.filtrarTemperatura(ms5);
		MedicoesSensores ms6 = new MedicoesSensores("\"tmp\"", "\"24.70\"", "\"2020-5-6 9:22:02\"");
		fm.filtrarTemperatura(ms6);
		MedicoesSensores ms7 = new MedicoesSensores("\"tmp\"", "\"24.50\"", "\"2020-5-6 9:22:02\"");
		fm.filtrarTemperatura(ms7);
		MedicoesSensores ms8 = new MedicoesSensores("\"tmp\"", "\"24.10\"", "\"2020-5-6 9:22:02\"");
		fm.filtrarTemperatura(ms8);
		MedicoesSensores ms9 = new MedicoesSensores("\"tmp\"", "\"27\"", "\"2020-5-6 9:22:02\"");
		fm.filtrarTemperatura(ms9);
		MedicoesSensores ms10 = new MedicoesSensores("\"tmp\"", "\"41\"", "\"2020-5-6 9:22:02\"");
		fm.filtrarTemperatura(ms10);

	}
}
