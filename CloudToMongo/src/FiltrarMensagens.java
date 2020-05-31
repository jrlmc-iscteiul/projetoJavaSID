import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class FiltrarMensagens {

	CloudToMongo cloudToMongo;

	private Stack<Double> medicoesLuminosidadeAnteriores = new Stack<Double>();
	
	private MedicoesSensores medLuminosidadeLixo = null;
	private MedicoesSensores medicaoMovAnterior = null;
	private MedicoesSensores medicaoMovLixo = null;
	
	private Stack<Double> lastTemperaturas = new Stack<Double>();
	private Stack<Double> lastHumidades = new Stack<Double>();
	
	private boolean haLuminosidade;
	private boolean haMovimento;
	
//	private JavaMysql mysql = new JavaMysql();

	public FiltrarMensagens(CloudToMongo cloudToMongo) {
		this.cloudToMongo = cloudToMongo;
	}

	private void inserirNaStack(MedicoesSensores medicao, Stack<Double> last) {
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		last.push(valor);
		if (last.size() > 30) {
			last.remove(last.firstElement());
		}
	}

	public void filtrarTemperatura(MedicoesSensores medicao) throws InterruptedException {
		if(lastTemperaturas.size() < 1) {
			inserirNaStack(medicao, lastTemperaturas);
			
		}
		List<Double> limites = outliers(lastTemperaturas, lastTemperaturas.size());
		String v = medicao.getValorMedicao();
		System.out.println("Valor que chegou: " + v);
		double valor = Double.parseDouble(v.replace("\"", ""));
		if(valor < limites.get(0) || valor > limites.get(1)) {
			cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			System.out.println("lixo");
		} else {
			System.out.println("Temp foi aceite");
			inserirNaStack(medicao, lastTemperaturas);
			cloudToMongo.mongocolTmp.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			medicao.setMedia(mediaLast(lastTemperaturas));
			cloudToMongo.mysql.getBq().offer(medicao);
		}
	}

	public void filtrarHumidade(MedicoesSensores medicao) throws InterruptedException {
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
			System.out.println("Hum foi aceite");
			inserirNaStack(medicao, lastHumidades);
			medicao.setMedia(mediaLast(lastHumidades));
			cloudToMongo.mongocolHum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			cloudToMongo.mysql.getBq().offer(medicao);	
		}
	}

	public void movimento(MedicoesSensores medicaoMovAtual) throws InterruptedException {
		Double valorMedicaoMovAtual = MedicoesSensores.tirarAspasValorMedicao(medicaoMovAtual);
		Double valorMedicaoMovAnterior;
		
		if (medicaoMovAnterior != null) {
			
			valorMedicaoMovAnterior = MedicoesSensores.tirarAspasValorMedicao(medicaoMovAnterior);
			
			if ((valorMedicaoMovAnterior == 0 && valorMedicaoMovAtual == 0)
					|| (valorMedicaoMovAnterior == 1 && valorMedicaoMovAtual == 0)
					|| (valorMedicaoMovAnterior == 1 && valorMedicaoMovAtual == 1)) {
				
				haMovimento = false;
				
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				cloudToMongo.mysql.getBq().offer(medicaoMovAtual);
				
				System.out.println("bom movimento");
								
				medicaoMovAnterior = medicaoMovAtual;
			}
			
			if (valorMedicaoMovAnterior == 0 && valorMedicaoMovAtual == 1 && !haMovimento) {
				haMovimento = true;
				
				cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				medicaoMovLixo = medicaoMovAtual;
				
				System.out.println("lixo movimento");
				
			} else if (valorMedicaoMovAnterior == 0 && valorMedicaoMovAtual == 1 && haMovimento) {
				
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovLixo.toString())));
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				
				cloudToMongo.mysql.getBq().offer(medicaoMovLixo);
				cloudToMongo.mysql.getBq().offer(medicaoMovAtual);
				
				cloudToMongo.mongocolLixo.findAndRemove((DBObject) JSON.parse(new String("{$and: [{dat:" + medicaoMovLixo.getData() + "}, {mov:" + medicaoMovLixo.getValorMedicao() + "}]}")));
				
				medicaoMovAnterior = medicaoMovAtual;
				
				System.out.println("bom movimento e tirar do lixo");
			}
		} else { // movimentoAnterior == null
			cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
			cloudToMongo.mysql.getBq().offer(medicaoMovAtual);
			medicaoMovAnterior = medicaoMovAtual;
			System.out.println("bom movimento");
		}
	}

	public void luminosidade(MedicoesSensores medicaoAtual) throws InterruptedException {
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
//				mysql.putDataIntoMysql(medicaoAtual, cloudToMongo.mongocolLum); // mudar valor media
				cloudToMongo.mysql.getBq().offer(medicaoAtual);

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
//				mysql.putDataIntoMysql(medicaoAtual, cloudToMongo.mongocolLum); // mudar valor media
				cloudToMongo.mysql.getBq().offer(medicaoAtual);

			} else if (haLuminosidade && ((valorMedLuminosidadeLixo - 10) <= valorMedicaoAtual)) {
				
				System.out.println("3º if");
				
				cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				cloudToMongo.mongocolLixo.findAndRemove((DBObject) JSON.parse(new String("{$and: [{dat:" + medLuminosidadeLixo.getData() + "}, {mov:" + medLuminosidadeLixo.getValorMedicao() + "}]}")));

//				mysql.putDataIntoMysql(medLuminosidadeLixo, cloudToMongo.mongocolLum); // mudar valor media
//				mysql.putDataIntoMysql(medicaoAtual, cloudToMongo.mongocolLum); // mudar valor media
				cloudToMongo.mysql.getBq().offer(medLuminosidadeLixo);
				cloudToMongo.mysql.getBq().offer(medicaoAtual);

				atualizarStackLuminosidade(medLuminosidadeLixo);
				atualizarStackLuminosidade(medicaoAtual);

				haLuminosidade = false;
			}
		} else { 
			
			cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
//			mysql.putDataIntoMysql(medicaoAtual, cloudToMongo.mongocolLum); //mudar valor media
			cloudToMongo.mysql.getBq().offer(medicaoAtual);
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
	
	private double mediaLast(Stack<Double> last) {
		System.out.println("media");
		double sum = 0;
		for (int i = 1; i < last.size(); i++) {
			double variacao = last.get(i) - last.get(i - 1);
			sum = sum + variacao;
		}
		return sum / last.size();
	}

	public static void main(String[] args) {

	}
}
