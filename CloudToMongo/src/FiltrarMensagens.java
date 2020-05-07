import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.Vector;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class FiltrarMensagens {
	
	CloudToMongo cloudToMongo;

	private Stack<Double> lastHumidades = new Stack<Double>();
	private Stack<Double> lastTemperaturas = new Stack<Double>();

	private MedicoesSensores medicaoMovAnterior;

	private boolean haMovimento;

	private Vector<Double> medicoesLuminosidadeAnteriores;

	private boolean haLuminosidade;

	private Double medLuminosidadeLixo;

	private String medLuminosidadelixoData;
	
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
		System.out.println(last);
	}
	
	public void filtrarTemperatura(MedicoesSensores medicao) {
		inserirNaStack(medicao, lastHumidades);
		List<Double> limites = new ArrayList<>();
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		if(valor < limites.get(0) || valor > limites.get(1)) {
			cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicao.toString())));
			System.out.println("lixo");
		} else {
			inserirNaStack(medicao, lastTemperaturas);
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
	
	public void movimento(MedicoesSensores medicaoMovAtual) {
		System.out.println("filtrar msgsss movimento");
		boolean valorAnterior0 = medicaoMovAnterior.getValorMedicao().equals("0");
		boolean valorAtual0 = medicaoMovAtual.getValorMedicao().equals("0");
		boolean valorAnterior1 = medicaoMovAnterior.getValorMedicao().equals("1");
		boolean valorAtual1 = medicaoMovAtual.getValorMedicao().equals("1");
		if (medicaoMovAnterior != null) {
			System.out.println("movimento anterior null");
			if ((valorAnterior0 && valorAtual0) || (valorAnterior1 && valorAtual0) || (valorAnterior1 && valorAtual1)) {
				haMovimento = false;
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				System.out.println("meteu na normal!!!");
			}
			if (valorAnterior0 && valorAtual1 && !haMovimento) {
				haMovimento = true;
				cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
				System.out.println("meteu no lixo!!!");
			} else if (valorAnterior0 && valorAtual1 && haMovimento) {
				// ir buscar a ultima msg do movimento ao lixo
				cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
			}
			medicaoMovAnterior = medicaoMovAtual;
		} else { // movimentoAnterior == null
			System.out.println("filtrar msgs else");
			cloudToMongo.mongocolMov.insert((DBObject) JSON.parse(cloudToMongo.clean(medicaoMovAtual.toString())));
			medicaoMovAnterior = medicaoMovAtual;
		}
	}

	public void luminosidade(MedicoesSensores medicaoAtual) {
		if (medicoesLuminosidadeAnteriores.size() == 3) {
			medicoesLuminosidadeAnteriores.remove(0);
			medicoesLuminosidadeAnteriores.add((double) Integer.parseInt(medicaoAtual.getValorMedicao()));
			if ((medicoesLuminosidadeAnteriores.get(1) - medicoesLuminosidadeAnteriores.get(0)) < Math.abs(10)
					&& (medicoesLuminosidadeAnteriores.get(3) - medicoesLuminosidadeAnteriores.get(2)) < Math.abs(10)) {
				cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicoesLuminosidadeAnteriores.get(0).toString())));
			}
			if ((medicoesLuminosidadeAnteriores.get(1) - medicoesLuminosidadeAnteriores.get(0)) < 10 && (medicoesLuminosidadeAnteriores.get(3) - medicoesLuminosidadeAnteriores.get(2)) > 50 && !haLuminosidade) {
				haLuminosidade = true;
				medLuminosidadeLixo = medicoesLuminosidadeAnteriores.get(0);
				medLuminosidadelixoData = medicaoAtual.getData();
				cloudToMongo.mongocolLixo.insert((DBObject) JSON.parse(cloudToMongo.clean(medicoesLuminosidadeAnteriores.get(0).toString())));
			} else if (haLuminosidade && ((medLuminosidadeLixo - 10) <= medicoesLuminosidadeAnteriores.get(0))) {
				// ir buscar a ultima msg do luminosidade ao lixo
				cloudToMongo.mongocolLum.insert((DBObject) JSON.parse(cloudToMongo.clean(medicoesLuminosidadeAnteriores.get(0).toString())));
			}
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
		if (size % 2 == 0) {
			q1 = (stackOrdenada.elementAt(mid1 + mid1 / 2) + stackOrdenada.elementAt(mid1 + (mid1 / 2) + 1)) / 2;
			q3 = (stackOrdenada.elementAt(mid2 - mid2 / 2) + stackOrdenada.elementAt(mid2 - (mid2 / 2) + 1)) / 2;
		} else {
			q1 = (stackOrdenada.elementAt(mid1 + mid1 / 2)/2);
			q3 = (stackOrdenada.elementAt(mid1 - mid1 / 2)/2);
		}
		double aiq = q3 - q1;
		System.out.println(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(size-2));
		if(between(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(size-2),0,2)) {
			limites.add((q1-aiq*12)-3);
			limites.add((q3+aiq*12)+3);
		} else if(between(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(size-2),2,5)) {
			limites.add(q1-aiq*6);
			limites.add(q3+aiq*6);
		} else {
			limites.add(q1-aiq*4);
			limites.add(q3+aiq*4);
		}
		System.out.println(limites);
		return limites;
	}
	
	private boolean between(double d, int min, int max) {
	    return (d >= min && d < max);
	}
	
	private Stack<Double> ordenarStack(Stack<Double> in) {
		Stack<Double> stackOrdenada = new Stack<>();
		while(!in.isEmpty()) { 
            double tmp = in.pop(); 
            while(!stackOrdenada.isEmpty() && stackOrdenada.peek() < tmp)  { 
            	in.push(stackOrdenada.pop()); 
            } 
            stackOrdenada.push(tmp); 
        }
		return stackOrdenada;
	}
}
