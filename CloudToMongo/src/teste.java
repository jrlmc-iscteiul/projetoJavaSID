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
		
//		subida.push((double) 21);
//		subida.push((double) 21);
//		subida.push((double) 20.5);
//		subida.push((double) 21);
//		subida.push((double) 21.5);
//		subida.push((double) 22);
//		subida.push((double) 22.5);
//		subida.push((double) 23);
//		subida.push((double) 23.5);
//		subida.push((double) 24);
//		subida.push((double) 24.5);
//		subida.push((double) 25);
//		subida.push((double) 25.5);
//		subida.push((double) 26);
//		subida.push((double) 26.5);
//
//		System.out.println(subida + " size:" + subida.size());
//		System.out.println("Media normal: " + mediaVariacoes(27, subida));
//		System.out.println("Media com oulier: " + mediaVariacoes(40, subida));
//		System.out.println(subida + "\n");
		
		normal.push((double) 21.5);
		normal.push((double) 21.5);
		normal.push((double) 21.5);
		normal.push((double) 21.5);
		normal.push((double) 21.5);
		normal.push((double) 21.5);
		normal.push((double) 21.5);
		normal.push((double) 21.5);
		normal.push((double) 21.5);
		normal.push((double) 21.5);
		normal.push((double) 21.5);
		normal.push((double) 21.5);

		
		System.out.println(normal + " size:" + normal.size());
		outliers(normal);
		System.out.println("Media normal: " + mediaVariacoes(21.3, normal));
		System.out.println("Media com oulier: " + mediaVariacoes(30, normal));
		System.out.println(normal);

		
	}

	//havia um problema porque se os valores fossem sempre os mesmos ia dar a cena dos outliers ia dar 0 e os limites iam ficar limitados a 1 só valor
	public static List<Double> outliers(Stack<Double> last) {
		Stack<Double> copy = new Stack<Double>();
		copy.addAll(last);
		Stack<Double> stackOrdenada = ordenarStack(copy);
		List<Double> limites = new ArrayList<>();
		double q1 = (stackOrdenada.elementAt(8) + stackOrdenada.elementAt(9))/2;
		double q3 = (stackOrdenada.elementAt(2) + stackOrdenada.elementAt(3))/2;
		double aiq = q3 - q1;
		if(between(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(9),0,2)) {
			limites.add((q1-aiq*20)+2);
			limites.add((q3+aiq*20)+2);
		} else if(between(stackOrdenada.elementAt(2) - stackOrdenada.elementAt(9),2,5)) {
			limites.add(q1-aiq*6);
			limites.add(q3+aiq*6);
		} else {
			limites.add(q1-aiq*4);
			limites.add(q3+aiq*4);
		}
		System.out.println(limites);
		return limites;
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
		return stackOrdenada;
	}

	public static double mediaVariacoes(double valor, Stack<Double> last) {
		last.push(valor);
		if (last.size() > 10) {
			last.remove(last.firstElement());
		}
		double sum = 0;
		for (int i = 1; i < last.size(); i++) {
			double variacao = last.get(i) - last.get(i - 1);
			sum = sum + variacao;
		}
		return sum / last.size();
	}

}
