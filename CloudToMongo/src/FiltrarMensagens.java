import java.awt.PageAttributes.MediaType;
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

	private boolean haLuminosidade;
	
	private MedicoesSensores medicaoMovAnterior = null;
	private boolean haMovimento;

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
		if (last.size() > 12) {
			last.remove(last.firstElement());
		}
		System.out.println(last);
	}

	public void filtrarTemperatura(MedicoesSensores medicao) {
		inserirNaStack(medicao, lastHumidades);
		List<Double> limites = new ArrayList<>();
		if (lastTemperaturas.size() > 6 && lastTemperaturas.size() % 2 == 0) {
			limites = outliers(lastTemperaturas, lastTemperaturas.size());
		}

		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		if (valor < limites.get(0) || valor > limites.get(1)) {
			cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			System.out.println("lixo");
		} else {
			cloudToMongo.mongocolTmp.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
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

	public void movimento (MedicoesSensores medicaoMovAtual) {
		
		System.out.println("filtrar msgsss movimento");

		if (medicaoMovAnterior != null) {
			
			System.out.println("movimento anterior null");

			if ((medicaoMovAnterior.getValorMedicao().equals("0") && medicaoMovAtual.getValorMedicao().equals("0"))
					|| (medicaoMovAnterior.getValorMedicao().equals("1") && medicaoMovAtual.getValorMedicao().equals("0"))
					|| (medicaoMovAnterior.getValorMedicao().equals("1") && medicaoMovAtual.getValorMedicao().equals("1"))) {
				haMovimento = false;
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				JavaMysql.putDataIntoMysql(medicaoMovAtual, 0);		//mudar o double
				System.out.println("meteu na normal!!!");
			}

			if (medicaoMovAnterior.getValorMedicao().equals("0") && medicaoMovAtual.getValorMedicao().equals("1") && !haMovimento) {
				haMovimento = true;
				
				cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				JavaMysql.putDataIntoMysql(medicaoMovAtual, 0);		//mudar o double
				
				System.out.println("meteu no lixo!!!");

			} else if (medicaoMovAnterior.getValorMedicao().equals("0") && medicaoMovAtual.getValorMedicao().equals("1") && haMovimento) {
				
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAnterior.toString())));
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				
				JavaMysql.putDataIntoMysql(medicaoMovAnterior, 0);		//mudar valor media
				JavaMysql.putDataIntoMysql(medicaoMovAtual, 0);			//mudar valor media
				
				cloudToMongo.mongocolLixo.findAndRemove((DBObject) JSON.parse(new String("{$and: [{dat:" + medicaoMovAnterior.getData() + "}, {mov:" + medicaoMovAnterior.getValorMedicao() + "}]}")));
			}

			medicaoMovAnterior = medicaoMovAtual;
			
		} else { 									// movimentoAnterior == null
			
			System.out.println("filtrar msgs else");
			cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
			JavaMysql.putDataIntoMysql(medicaoMovAtual, 0);		//mudar valor media
			
			medicaoMovAnterior = medicaoMovAtual;
		}
	}

	public void luminosidade(MedicoesSensores medicaoAtual) {
		
		Double valorMedLuminosidadeLixo = null;
		if(medLuminosidadeLixo != null)
			valorMedLuminosidadeLixo = MedicoesSensores.tirarAspasValorMedicao(medLuminosidadeLixo);
	
		Double valorMedicaoAtual = MedicoesSensores.tirarAspasValorMedicao(medicaoAtual);
		System.out.println(valorMedicaoAtual);
		
		if(medicoesLuminosidadeAnteriores.size() == 2) {
			
			if( (medicoesLuminosidadeAnteriores.get(1)-medicoesLuminosidadeAnteriores.get(0)) < Math.abs(10) 
					&& (valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) < Math.abs(10) ) {
				cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				JavaMysql.putDataIntoMysql(medicaoAtual, 0);			//mudar valor media
				atualizarStackLuminosidade(medicaoAtual);
			}
			
			if( (medicoesLuminosidadeAnteriores.get(1) - valorMedicaoAtual) < 10 
					&& ((valorMedicaoAtual - medicoesLuminosidadeAnteriores.get(1)) > 50 && !haLuminosidade) ) {
				
				haLuminosidade = true;
				medLuminosidadeLixo = medicaoAtual;
				
				cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				JavaMysql.putDataIntoMysql(medicaoAtual, 0);			//mudar valor media
				
			} else if (haLuminosidade && ((valorMedLuminosidadeLixo-10) <= valorMedicaoAtual)) {
				
				cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				cloudToMongo.mongocolLixo.findAndRemove((DBObject) JSON.parse(new String("{$and: [{dat:" + medLuminosidadeLixo.getData() + "}, {mov:" + medLuminosidadeLixo.getValorMedicao() + "}]}")));
				
				JavaMysql.putDataIntoMysql(medLuminosidadeLixo, 0);		//mudar valor media
				JavaMysql.putDataIntoMysql(medicaoAtual, 0);			//mudar valor media
								
				atualizarStackLuminosidade(medLuminosidadeLixo); 
				atualizarStackLuminosidade(medicaoAtual);
			
			}
		} else {			//fazer diferente p tamanho 1 ?
				cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoAtual.toString())));
				JavaMysql.putDataIntoMysql(medicaoAtual, 0);			//mudar valor media
		}
	}

	public List<Double> outliers(Stack<Double> last, int size) {
		Stack<Double> copy = new Stack<Double>();
		copy.addAll(last);
		Stack<Double> stackOrdenada = ordenarStack(copy);
		List<Double> limites = new ArrayList<>();

		double q1 = (stackOrdenada.elementAt((size / 2) - 3) + stackOrdenada.elementAt((size / 2) - 4)) / 2;
		double q3 = (stackOrdenada.elementAt((size / 2) + 2) + stackOrdenada.elementAt((size / 2) + 3)) / 2;
		double aiq = q3 - q1;
		if (between(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(size - 2), 0, 2)) {
			limites.add((q1 - aiq * 20) + 2);
			limites.add((q3 + aiq * 20) + 2);
		} else if (between(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(9), 2, 5)) {
			limites.add(q1 - aiq * 6);
			limites.add(q3 + aiq * 6);
		} else {
			limites.add(q1 - aiq * 4);
			limites.add(q3 + aiq * 4);
		}
		System.out.println(limites);
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
	
	
	private void atualizarStackLuminosidade (MedicoesSensores medicao) {
		medicoesLuminosidadeAnteriores.remove(0);
		Double valorMedicao = MedicoesSensores.tirarAspasValorMedicao(medicao);
		medicoesLuminosidadeAnteriores.add(valorMedicao);
	}
}
