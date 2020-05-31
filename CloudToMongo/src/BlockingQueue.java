import java.util.LinkedList;
import java.util.Queue;

public class BlockingQueue<T> {

	private Queue<T> fila = new LinkedList<T>();

	public synchronized void offer(T t) throws InterruptedException {
		fila.add(t);
		notifyAll();
	}

	public synchronized T take() throws InterruptedException {
		while (fila.isEmpty()) {
			wait();
		}
		return fila.poll();
	}

}