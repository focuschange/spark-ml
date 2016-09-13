package org.irgroup.datastructure;

/**
 * <pre>
 *      org.irgroup.datastructure
 *        |_ MaxHeap.java
 * </pre>
 * <p>
 * <pre>
 *
 * </pre>
 *
 * @Author : 이상호(focuschange@gmail.com)
 * @Date : 16. 9. 5.
 * @modified :
 * @Version : 1.0
 * @see : http://people.cs.vt.edu/shaffer/Book/JAVA/progs/Grkrusm/MinHeap.java
 */

public class MaxHeap<E extends Comparable<? super E>> {
	private E[] Heap;   // Pointer to the heap array
	private int size;   // Maximum size of the heap
	private int n;      // Number of things in heap

	public MaxHeap(E[] h, int num, int max) {
		Heap = h;
		n = num;
		size = max;
		build();
	}

	/**
	 * Return current number of things in heap
	 */
	public int count() {
		return n;
	}

	/**
	 * Is pos a leaf position?
	 */
	private boolean isLeaf(int pos) {
		return (pos >= n / 2) && (pos < n);
	}

	/**
	 * Return position for left child of pos
	 */
	private int leftChild(int pos) {
		if (pos >= (n / 2))
			return -1; //Position has no left child

		return 2 * pos + 1;
	}

	/**
	 * Return position for right child of pos
	 */
	private int rightChild(int pos) {
		if (pos >= ((n - 1) / 2))
			return -1; // Position has no right child

		return 2 * pos + 2;
	}

	/**
	 * Return position for parent
	 */
	private int parent(int pos) {
		assert pos > 0 : "Position has no parent";
		return (pos - 1) / 2;
	}

	/**
	 * Heapify contents of Heap
	 */
	public void build() {
		for (int i = n / 2 - 1; i >= 0; i--)
			siftDown(i);
	}

	private void swap(int fpos, int spos) {
		E tmp = Heap[fpos];
		Heap[fpos] = Heap[spos];
		Heap[spos] = tmp;
	}

	/**
	 * Insert into heap
	 */
	public void insert(E val) {
		if (n == size)   // 가장 큰 것을 버림
		{
			if (Heap[0].compareTo(val) < 0)
				return;

			remove();
			insert(val);
		} else {
			int curr = n++;
			Heap[curr] = val;                 // Start at end of heap

			// Now sift up until curr's parent's key > curr's key
			while ((curr != 0) && (Heap[curr].compareTo(Heap[parent(curr)]) > 0)) {
				swap(curr, parent(curr));
				curr = parent(curr);
			}
		}
	}

	/**
	 * Put element in its correct place
	 */
	private void siftDown(int pos) {
		assert (pos >= 0) && (pos < n) : "Illegal heap position";

		while (!isLeaf(pos)) {
			int j = leftChild(pos);
			if ((j < (n - 1)) && (Heap[j].compareTo(Heap[j + 1]) < 0))
				j++; // j is now index of child with greater value

			if (Heap[pos].compareTo(Heap[j]) > 0)
				return;

			swap(pos, j);
			pos = j;  // Move down
		}
	}

	public E remove() {     // Remove minimum value
		assert n > 0 : "Removing from empty heap";

		swap(0, --n); // Swap minimum with last value
		if (n != 0)      // Not on last element
			siftDown(0);   // Put new heap root val in correct place

		return Heap[n];
	}

	/**
	 * Remove element at specified position
	 */
	public E remove(int pos) {
		assert (pos >= 0) && (pos < n) : "Illegal heap position";

		if (pos == (n - 1))
			n--; // Last element, no work to be done
		else {
			swap(pos, --n); // Swap with last value

			// If we just swapped in a small value, push it up
			while ((pos > 0) && (Heap[pos].compareTo(Heap[parent(pos)]) < 0)) {
				swap(pos, parent(pos));
				pos = parent(pos);
			}

			if (n != 0)
				siftDown(pos);   // If it is big, push down
		}
		return Heap[n];
	}

	public void print() {
		System.out.println("heap size : " + count());

		for (int i = 0; i <= size / 2; i++) {
			System.out.print(" PARENT : " + Heap[i]);

			int left = i * 2 + 1;
			int right = i * 2 + 2;

			if(left < size && Heap[left] != null)
				System.out.print(" LEFT CHILD : " + Heap[left]);
			if(right < size && Heap[right] != null)
				System.out.print(" RIGHT CHILD :" + Heap[right]);
			System.out.println();
		}
	}
}

