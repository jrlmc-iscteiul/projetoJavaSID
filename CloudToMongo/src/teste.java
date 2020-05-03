import java.util.Stack;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class teste {
	private static Stack<Double> subida;
	private static Stack<Double> normal;
	public static void main(String[] args) {
		subida = new Stack<>();
		normal = new Stack<>();
		
		subida.push((double) 21);
		subida.push((double) 21);
		subida.push((double) 20.5);
		subida.push((double) 21);
		subida.push((double) 21.5);
		subida.push((double) 22);
		subida.push((double) 22.5);
		subida.push((double) 23);
		subida.push((double) 23.5);
		subida.push((double) 24);
		subida.push((double) 24.5);
		subida.push((double) 25);
		subida.push((double) 25.5);
		subida.push((double) 26);
		subida.push((double) 26.5);

		System.out.println("Em subida: " + subida + " size:" + subida.size());
		System.out.println("Media: " + mediasubida(34, subida));
		System.out.println(subida);
		
		normal.push((double) 21);
		normal.push((double) 21.2);
		normal.push((double) 21);
		normal.push((double) 21.2);
		normal.push((double) 21.2);
		normal.push((double) 21.3);
		normal.push((double) 21.2);
		normal.push((double) 21.1);
		normal.push((double) 21.2);
		normal.push((double) 21.3);
		normal.push((double) 21.4);
		normal.push((double) 21);
		normal.push((double) 21.2);
		normal.push((double) 21.2);
		normal.push((double) 21.3);
		
		System.out.println("Estado normal: " + normal + " size:" + normal.size());
		System.out.println("Media: " + mediasubida(21.5, normal));
		System.out.println(normal);
		
		
		
	}

	

	public static double mediasubida(double valor, Stack<Double> last) {
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
