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

	public FiltrarMensagens(CloudToMongo cloudToMongo) {
		this.cloudToMongo = cloudToMongo;
	}

	
	//Para o c�lculo dos outliers definimos a utiliza��o das ultimas medi��es, pelo que a Stack � limitada a 30 valores, 
	//pelo que a Stack � limitada a 30 valores e sempre que chega um novo valor � colocado na frente de fila retirando o valor mais antigo
	private void inserirNaStack(MedicoesSensores medicao, Stack<Double> last) {
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		last.push(valor);
		if (last.size() > 30) {
			last.remove(last.firstElement());
		}
	}

	//M�todo que faz a filtragem da temperatura. S�o utilizadas Stacks para guardar apenas os ultimos 30 valores de medi��es, do tipo Double,
	//por ordem de chegada, para serem utilizados para calcular o valor dos outliers e a m�dia das varia��es
	//Caso o valor esteja fora dos limites dos outliers ser� colocado na cole��o das mensagens descartadas e caso seja aceite,
	//� colocado na Stack das ultimas medi��es, na respetiva cole��o mongo e da BloquingQueue onde vai esperar para ser retirada e enviada para o mysql.
	public void filtrarTemperatura(MedicoesSensores medicao) throws InterruptedException {
		if(lastTemperaturas.size() < 1) {
			inserirNaStack(medicao, lastTemperaturas);
		}
		List<Double> limites = outliers(lastTemperaturas, lastTemperaturas.size());
		String v = medicao.getValorMedicao();
		System.out.println("Temperatura: Valor: " + v + " Limites: " + limites);
		double valor = Double.parseDouble(v.replace("\"", ""));
		if(valor < limites.get(0) || valor > limites.get(1)) {
			cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			System.out.println("Temperatura descartada");
		} else {
			System.out.println("Temperatura aceite");
			inserirNaStack(medicao, lastTemperaturas);
			cloudToMongo.mongocolTmp.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			medicao.setMedia(mediaLast(lastTemperaturas));
			cloudToMongo.mysql.getBq().offer(medicao);
		}
	}

	//M�todo que faz a filtragem dos valores de humidade. Estrutura semelhante � do filtro da temperatura
	public void filtrarHumidade(MedicoesSensores medicao) throws InterruptedException {
		if(lastHumidades.size() < 1) {
			inserirNaStack(medicao, lastHumidades);
		}
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		List<Double> limites = outliers(lastHumidades, lastHumidades.size());
		System.out.println("Humiadade: Valor: " + v + " Limites: " + limites);
		if(valor < limites.get(0) || valor > limites.get(1) || valor < 0 || valor > 100) {
			cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			System.out.println("Humidade descartada");
		} else {
			System.out.println("Humidade aceite");
			inserirNaStack(medicao, lastHumidades);
			medicao.setMedia(mediaLast(lastHumidades));
			cloudToMongo.mongocolHum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			cloudToMongo.mysql.getBq().offer(medicao);	
		}
	}

	public void movimento(MedicoesSensores medicaoMovAtual) throws InterruptedException {
		
		Double valorMedicaoMovAtual = MedicoesSensores.tirarAspasValorMedicao(medicaoMovAtual);
		Double valorMedicaoMovAnterior;
		
		if(valorMedicaoMovAtual != 0 || valorMedicaoMovAtual != 1) {
			cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
			return;
		}
		
		if (medicaoMovAnterior != null) {
			
			valorMedicaoMovAnterior = MedicoesSensores.tirarAspasValorMedicao(medicaoMovAnterior);
			
			if ((valorMedicaoMovAnterior == 0 && valorMedicaoMovAtual == 0)
					|| (valorMedicaoMovAnterior == 1 && valorMedicaoMovAtual == 0)
					|| (valorMedicaoMovAnterior == 1 && valorMedicaoMovAtual == 1)) {
				
				haMovimento = false;
				
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				cloudToMongo.mysql.getBq().offer(medicaoMovAtual);
				
				System.out.println("Movimento aceite");
								
				medicaoMovAnterior = medicaoMovAtual;
			}
			
			if (valorMedicaoMovAnterior == 0 && valorMedicaoMovAtual == 1 && !haMovimento) {
				haMovimento = true;
				
				cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				medicaoMovLixo = medicaoMovAtual;
				
				System.out.println("Movimento descartado");
				
			} else if (valorMedicaoMovAnterior == 0 && valorMedicaoMovAtual == 1 && haMovimento) {
				
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovLixo.toString())));
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				
				cloudToMongo.mysql.getBq().offer(medicaoMovLixo);
				cloudToMongo.mysql.getBq().offer(medicaoMovAtual);
				
				cloudToMongo.mongocolLixo.findAndRemove((DBObject) JSON.parse(new String("{$and: [{dat:" + medicaoMovLixo.getData() + "}, {mov:" + medicaoMovLixo.getValorMedicao() + "}]}")));
				
				medicaoMovAnterior = medicaoMovAtual;
				
				System.out.println("Movimento aceite");
			}
		} else { // movimentoAnterior == null
			cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
			cloudToMongo.mysql.getBq().offer(medicaoMovAtual);
			medicaoMovAnterior = medicaoMovAtual;
			System.out.println("Movimento aceite");
		}
	}

	public void luminosidade(MedicoesSensores medicaoAtual) throws InterruptedException {
		
		Double valorMedicaoAtual = MedicoesSensores.tirarAspasValorMedicao(medicaoAtual);
		
		if(valorMedicaoAtual < 0) {
			cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
			return;
		}
			
		Double valorMedLuminosidadeLixo = null;
		
		if (medLuminosidadeLixo != null)
			valorMedLuminosidadeLixo = MedicoesSensores.tirarAspasValorMedicao(medLuminosidadeLixo);

		if (medicoesLuminosidadeAnteriores.size() == 2) {

			if (((medicoesLuminosidadeAnteriores.get(1) - medicoesLuminosidadeAnteriores.get(0)) <= Math.abs(10) && (valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) <= Math.abs(10))
					|| ((medicoesLuminosidadeAnteriores.get(1) - medicoesLuminosidadeAnteriores.get(0)) <= Math.abs(10) && (valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) <= Math.abs(50))
					|| ((medicoesLuminosidadeAnteriores.get(1) - medicoesLuminosidadeAnteriores.get(0)) <= Math.abs(50) && (valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) <= Math.abs(10))
					|| ((medicoesLuminosidadeAnteriores.get(1) - medicoesLuminosidadeAnteriores.get(0)) <= Math.abs(50) && (valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) <= Math.abs(50)) ) {	

				cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				cloudToMongo.mysql.getBq().offer(medicaoAtual);

				atualizarStackLuminosidade(medicaoAtual);
				haLuminosidade = false;
				System.out.println("Luminosidade aceite");
			}

			if ((medicoesLuminosidadeAnteriores.get(1) - valorMedicaoAtual) <= Math.abs(10)
					&& ((valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) > Math.abs(50) && !haLuminosidade)) {
				
				haLuminosidade = true;
				medLuminosidadeLixo = medicaoAtual;

				cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				cloudToMongo.mysql.getBq().offer(medicaoAtual);
				System.out.println("Luminosidade descartada");

			} else if (haLuminosidade && ((valorMedLuminosidadeLixo - 10) <= valorMedicaoAtual)) {

				cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				cloudToMongo.mongocolLixo.findAndRemove((DBObject) JSON.parse(new String("{$and: [{dat:" + medLuminosidadeLixo.getData() + "}, {mov:" + medLuminosidadeLixo.getValorMedicao() + "}]}")));

				cloudToMongo.mysql.getBq().offer(medLuminosidadeLixo);
				cloudToMongo.mysql.getBq().offer(medicaoAtual);

				atualizarStackLuminosidade(medLuminosidadeLixo);
				atualizarStackLuminosidade(medicaoAtual);
				
				haLuminosidade = false;
				System.out.println("Luminosidade aceite");
			}
		} else { 
			
			cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
			cloudToMongo.mysql.getBq().offer(medicaoAtual);
			atualizarStackLuminosidade(medicaoAtual);
			System.out.println("Luminosidade aceite");
		}
	}

	
	//Esta fun��o vai receber a Stack das �ltimas medi��es e orden�-la por valores utilizando uma Stack auxiliar. 
	//Depois s�o calculados os quartis Q1 e Q3, para calcular o desvio-quartil AIQ.
	//A partir de Q1, Q3 e AIQ s�o calculados os valores limite para aceitar as medic�es,
	//utilizando um multiplicador diferente que tem em conta a amplitude dos valores na Stack ordenada
	//e o facto de a varia��o poder ser exponencial
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
	
	//Para c�lculo da m�dia das varia��es dos ultimos 30 valores
	private double mediaLast(Stack<Double> last) {
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
