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

	
	//Para o cálculo dos outliers definimos a utilização das ultimas medições, pelo que a Stack é limitada a 30 valores, 
	//pelo que a Stack é limitada a 30 valores e sempre que chega um novo valor é colocado na frente de fila retirando o valor mais antigo
	private void inserirNaStack(MedicoesSensores medicao, Stack<Double> last) {
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		last.push(valor);
		if (last.size() > 30) {
			last.remove(last.firstElement());
		}
	}

	//Método que faz a filtragem da temperatura. São utilizadas Stacks para guardar apenas os ultimos 30 valores de medições, do tipo Double,
	//por ordem de chegada, para serem utilizados para calcular o valor dos outliers e a média das variações
	//Caso o valor esteja fora dos limites dos outliers será colocado na coleção das mensagens descartadas e caso seja aceite,
	//é colocado na Stack das ultimas medições, na respetiva coleção mongo e da BloquingQueue onde vai esperar para ser retirada e enviada para o mysql.
	
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

	//Método que faz a filtragem dos valores de humidade. Estrutura semelhante à do filtro da temperatura
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
	 * Método que irá avaliar as medições do movimento, quais as medições que devem ou não ser descartadas
	 * Medições que o valor não seja 0 ou 1 serão descartadas
	 * Quando as medições estão a 0, e é enviado um 1, esse 1 vai ser enviado para a coleção msgsDescartadas, e essa mesma medição é guardada numa variável "medicaoMovAnterior"
	 * É também usado um boolean "haMovimento" que é posto a true quando uma medição vai para a coleção msgsDescartadas
	 * Depois existem duas situações:
	 * 1ª:  Se o valor seguinte for 0, então quer dizer que o 1 que foi para a coleção msgsDescartadas (medição anterior), foi colocado lá corretamente e que lá deve permanecer
	 * 		Para isso, comparamos o valor da medição atual com a medição anterior, vemos que é um 1 seguido de um 0, colocamos o boolean a false e a medição atual vai para a coleção movimento
	 * 2ª:  Se o valor seguinte for 1, então quer dizer que o 1 que foi para a coleção msgsDescartadas, foi colocado lá erradamente e tem de ser tirado e colocada na coleção movimento
	 * 		Para isso devemos, comparar o valor da medição anterior com o atual, ver se o boolean está a true, o que quer dizer que é preciso ir tirar uma medição da coleção msgsDescartadas
	 * 		Colocamos a medição atual e a anterior no MongoDB e removemos a medição anterior da coleção msgsDescartadas com o comando findAndRemove()
	 * As mensagens são colocadas nas coleções correspondentes através do comando insert() 
	 * Sempre que as medições forem colocadas no MongoDB também o são no Mysql através do método offer() à BlockingQueue
	 * Na primeira medição de todas a ser colocada, como não há medição anterior esta é colocada sempre na coleção movimento
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
				System.out.println("Movimento aceite e recupera a medição anterior");
			}
		} else { 																	// movimentoAnterior == null
			cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
			cloudToMongo.mysql.getBq().offer(medicaoMovAtual);
			medicaoMovAnterior = medicaoMovAtual;
			System.out.println("Movimento aceite");
		}
	}

	
	/*
	 * Método que irá avaliar as medições do movimento, quais as medições que devem ou não ser descartadas
	 * Medições com valores abaixo de 0 serão descartados
	 * Existe um boolean "haLuminosidade" para quando uma mensagem for colocada na coleção msgsDescartadas
	 * Existe um stack com as duas medições anteriores à medição atual para compararmos o aumento dos valores e perceber se a medição atual tem um valor adequado ou se é um erro do sensor
	 * Enquanto o stack não tem tamanho 2, as medições são colocadas na coleção luminosidade do MongoDB
	 * Quando o stack tem tamanho 2, quando é recebido um valor, averigua-se se a variação entre as últimas duas medições e a variação entre a última medição e a atual
	 * Existem três tipos de situação neste caso:
	 * 		1º: Quando a variação entre as últimas duas medições anteriores é menos que 10 e a variação entre o valor atual e a última medição é maior que 50
	 * 			Nesse caso, a mensagem atual é posta na coleção msgsDescartadas e o boolean é colocado a true
	 * 		2º: Quando o boolean haLuminosidade está a true e a variação entre a mensagem colocada na coleção msgsDescartadas e a medição atual for menor que 10
	 * 			Então a mensagem que estava na coleção msgsDescartadas é recuperada, e tanto esta mensagem como a mensagem atual são colocadas na coleção luminosidade
	 * 			Para recuperar a mensagem é usada o método findAndRemove() que vai procurar no MongoDB a mensagem que tem a data e hora correspondentes à mensagem que foi coloca nas msgsDescartadas
	 * 		3º: Todas as situações que não são descritas acima e sempre que o haLuminosidade estiver a false, as mensagens são colocadas na coleção luminosidade
	 * 	As mensagens são colocadas nas coleções correspondentes através do comando insert() 
	 * Sempre que as medições forem colocadas no MongoDB também o são no Mysql através do método offer() à BlockingQueue
	 * No final de cada if ou else if é atualizado o stack da luminosidade para este ter sempre as últimas duas medições, usa-se para isso o método atualizarStackLuminosidade()
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
				System.out.println("Luminosidade aceite e medição anterior recuperada");
			}
		} else { 											//medicoesLuminosidadeAnteriores.size < 2
			cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
			cloudToMongo.mysql.getBq().offer(medicaoAtual);
			atualizarStackLuminosidade(medicaoAtual);
			System.out.println("Luminosidade aceite");
		}
	}

	
	//Esta função vai receber a Stack das últimas medições e ordená-la por valores utilizando uma Stack auxiliar. 
	//Depois são calculados os quartis Q1 e Q3, para calcular o desvio-quartil AIQ.
	//A partir de Q1, Q3 e AIQ são calculados os valores limite para aceitar as medicões,
	//utilizando um multiplicador diferente que tem em conta a amplitude dos valores na Stack ordenada
	//e o facto de a variação poder ser exponencial
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
	
	//Para cálculo da média das variações dos ultimos 30 valores
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
