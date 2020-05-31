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
	
	/*
	 * M�todo que ir� avaliar as medi��es do movimento, quais as medi��es que devem ou n�o ser descartadas
	 * Medi��es que o valor n�o seja 0 ou 1 ser�o descartadas
	 * Quando as medi��es est�o a 0, e � enviado um 1, esse 1 vai ser enviado para a cole��o msgsDescartadas, e essa mesma medi��o � guardada numa vari�vel "medicaoMovAnterior"
	 * � tamb�m usado um boolean "haMovimento" que � posto a true quando uma medi��o vai para a cole��o msgsDescartadas
	 * Depois existem duas situa��es:
	 * 1�:  Se o valor seguinte for 0, ent�o quer dizer que o 1 que foi para a cole��o msgsDescartadas (medi��o anterior), foi colocado l� corretamente e que l� deve permanecer
	 * 		Para isso, comparamos o valor da medi��o atual com a medi��o anterior, vemos que � um 1 seguido de um 0, colocamos o boolean a false e a medi��o atual vai para a cole��o movimento
	 * 2�:  Se o valor seguinte for 1, ent�o quer dizer que o 1 que foi para a cole��o msgsDescartadas, foi colocado l� erradamente e tem de ser tirado e colocada na cole��o movimento
	 * 		Para isso devemos, comparar o valor da medi��o anterior com o atual, ver se o boolean est� a true, o que quer dizer que � preciso ir tirar uma medi��o da cole��o msgsDescartadas
	 * 		Colocamos a medi��o atual e a anterior no MongoDB e removemos a medi��o anterior da cole��o msgsDescartadas com o comando findAndRemove()
	 * As mensagens s�o colocadas nas cole��es correspondentes atrav�s do comando insert() 
	 * Sempre que as medi��es forem colocadas no MongoDB tamb�m o s�o no Mysql atrav�s do m�todo offer() � BlockingQueue
	 * Na primeira medi��o de todas a ser colocada, como n�o h� medi��o anterior esta � colocada sempre na cole��o movimento
	 */
	public void filtrarMovimento(MedicoesSensores medicaoMovAtual) throws InterruptedException {
		
		Double valorMedicaoMovAtual = MedicoesSensores.tirarAspasValorMedicao(medicaoMovAtual);
		Double valorMedicaoMovAnterior;
		
		if(valorMedicaoMovAtual != 0 || valorMedicaoMovAtual != 1) {
			cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
			return;
		}
		
		if (medicaoMovAnterior != null) {
			
			valorMedicaoMovAnterior = MedicoesSensores.tirarAspasValorMedicao(medicaoMovAnterior);
			
			if ((valorMedicaoMovAnterior == 0 && valorMedicaoMovAtual == 0) || (valorMedicaoMovAnterior == 1 && valorMedicaoMovAtual == 0) 
					|| (valorMedicaoMovAnterior == 1 && valorMedicaoMovAtual == 1)) {
				haMovimento = false;
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				cloudToMongo.mysql.getBq().offer(medicaoMovAtual);
				medicaoMovAnterior = medicaoMovAtual;
				System.out.println("Movimento aceite");
				
			} else if (valorMedicaoMovAnterior == 0 && valorMedicaoMovAtual == 1 && !haMovimento) {
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
				System.out.println("Movimento aceite e recupera a medi��o anterior");
			}
		} else { 																	// movimentoAnterior == null
			cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
			cloudToMongo.mysql.getBq().offer(medicaoMovAtual);
			medicaoMovAnterior = medicaoMovAtual;
			System.out.println("Movimento aceite");
		}
	}

	
	/*
	 * M�todo que ir� avaliar as medi��es do movimento, quais as medi��es que devem ou n�o ser descartadas
	 * Medi��es com valores abaixo de 0 ser�o descartados
	 * Existe um boolean "haLuminosidade" para quando uma mensagem for colocada na cole��o msgsDescartadas
	 * Existe um stack com as duas medi��es anteriores � medi��o atual para compararmos o aumento dos valores e perceber se a medi��o atual tem um valor adequado ou se � um erro do sensor
	 * Enquanto o stack n�o tem tamanho 2, as medi��es s�o colocadas na cole��o luminosidade do MongoDB
	 * Quando o stack tem tamanho 2, quando � recebido um valor, averigua-se se a varia��o entre as �ltimas duas medi��es e a varia��o entre a �ltima medi��o e a atual
	 * Existem tr�s tipos de situa��o neste caso:
	 * 		1�: Quando a varia��o entre as �ltimas duas medi��es anteriores � menos que 10 e a varia��o entre o valor atual e a �ltima medi��o � maior que 50
	 * 			Nesse caso, a mensagem atual � posta na cole��o msgsDescartadas e o boolean � colocado a true
	 * 		2�: Quando o boolean haLuminosidade est� a true e a varia��o entre a mensagem colocada na cole��o msgsDescartadas e a medi��o atual for menor que 10
	 * 			Ent�o a mensagem que estava na cole��o msgsDescartadas � recuperada, e tanto esta mensagem como a mensagem atual s�o colocadas na cole��o luminosidade
	 * 			Para recuperar a mensagem � usada o m�todo findAndRemove() que vai procurar no MongoDB a mensagem que tem a data e hora correspondentes � mensagem que foi coloca nas msgsDescartadas
	 * 		3�: Todas as situa��es que n�o s�o descritas acima e sempre que o haLuminosidade estiver a false, as mensagens s�o colocadas na cole��o luminosidade
	 * 	As mensagens s�o colocadas nas cole��es correspondentes atrav�s do comando insert() 
	 * Sempre que as medi��es forem colocadas no MongoDB tamb�m o s�o no Mysql atrav�s do m�todo offer() � BlockingQueue
	 * No final de cada if ou else if � atualizado o stack da luminosidade para este ter sempre as �ltimas duas medi��es, usa-se para isso o m�todo atualizarStackLuminosidade()
	 */
	public void filtrarLuminosidade(MedicoesSensores medicaoAtual) throws InterruptedException {
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
					|| ((medicoesLuminosidadeAnteriores.get(1) - medicoesLuminosidadeAnteriores.get(0)) <= Math.abs(50) && (valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) <= Math.abs(50)) ){	

				cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				cloudToMongo.mysql.getBq().offer(medicaoAtual);

				atualizarStackLuminosidade(medicaoAtual);
				haLuminosidade = false;
				System.out.println("Luminosidade aceite");
			}

			else if ((medicoesLuminosidadeAnteriores.get(1) - valorMedicaoAtual) <= Math.abs(10) && ((valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) > Math.abs(50) && !haLuminosidade)) {
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
				System.out.println("Luminosidade aceite e medi��o anterior recuperada");
			}
		} else { 											//medicoesLuminosidadeAnteriores.size < 2
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
