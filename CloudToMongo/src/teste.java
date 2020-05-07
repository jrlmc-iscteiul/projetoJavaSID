import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class teste {
	private static Stack<Double> subida;
	private static Stack<Double> normal;
	public static void main(String[] args) {
		subida = new Stack<>();
		normal = new Stack<>();

		
		normal.push((double) 21.5);
		normal.push((double) 21.5);
		normal.push((double) 21.2);
		normal.push((double) 22);
		normal.push((double) 21.5);
		normal.push((double) 22);
		normal.push((double) 22);
		normal.push((double) 22);
		normal.push((double) 21);
		normal.push((double) 21.5);
//		normal.push((double) 21.9);
//		normal.push((double) 22);
//		normal.push((double) 22);
//		normal.push((double) 21.8);
//		normal.push((double) 21.9);
//		normal.push((double) 21.7);
//		normal.push((double) 21.4);
//		normal.push((double) 21);
//		normal.push((double) 21);
//		normal.push((double) 21.2);
//		normal.push((double) 21.2);
//		normal.push((double) 26);
//		normal.push((double) 22);
//		normal.push((double) 22);
//		normal.push((double) 22);
//		normal.push((double) 21.7);
//		normal.push((double) 18.9);
//		normal.push((double) 21.5);
//		normal.push((double) 22);
//		normal.push((double) 21.7);


		
		System.out.println(normal + " size:" + normal.size());
		outliers(normal, normal.size());
		System.out.println(normal);
		System.out.println(5/2);

		
	}

	//havia um problema porque se os valores fossem sempre os mesmos ia dar a cena dos outliers ia dar 0 e os limites iam ficar limitados a 1 só valor
	public static List<Double> outliers(Stack<Double> last, int size) {
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
	
	private static void inserirNaStack(MedicoesSensores medicao, Stack<Double> last) {
		String v = medicao.getValorMedicao();
		double valor = Double.parseDouble(v.replace("\"", ""));
		last.push(valor);
		if (last.size() > 30) {
			last.remove(last.firstElement());
		}
		System.out.println(last);
	}
	
	public static boolean between(double d, int min, int max) {
	    return (d >= min && d < max);
	}
	
	public static Stack<Double> ordenarStack(Stack<Double> in) {
		Stack<Double> stackOrdenada = new Stack<>();
		while(!in.isEmpty()) { 
            double tmp = in.pop(); 
            while(!stackOrdenada.isEmpty() && stackOrdenada.peek() < tmp)  { 
            	in.push(stackOrdenada.pop()); 
            } 
            stackOrdenada.push(tmp); 
        }
		System.out.println(stackOrdenada);
		return stackOrdenada;
	}

	private static double mediaLast(Stack<Double> last) {
		double sum = 0;
		for (int i = 1; i < last.size(); i++) {
			double variacao = last.get(i) - last.get(i - 1);
			sum = sum + variacao;
		}
		return sum / last.size();
	}

}
