import java.util.Stack;

public class teste2 {
	
	private static double mediaLast(Stack<Double> last) {
		double sum = 0;
		for (int i = 1; i < last.size(); i++) {
			double variacao = last.get(i) - last.get(i - 1);
			System.out.println("variacao: " + variacao);
			sum = sum + variacao;
			System.out.println("sum: " + sum);
			System.out.println("size: " + last.size());
		}
		return sum / last.size();
	}
	
	public static void main(String[] args) {
		Stack<Double> stack = new Stack<>();
		stack.add(61.00);
		stack.add(62.00);
		stack.add(63.00);
		stack.add(66.00);
		stack.add(69.00);
		
		Double res = mediaLast(stack);
		System.out.println(res);
	}
}
