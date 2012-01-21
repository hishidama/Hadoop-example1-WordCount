package example1.mr;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CounterMap {
	private Map<String, Counter> map;

	public CounterMap() {
		map = new HashMap<String, Counter>();
	}

	public CounterMap(int size) {
		map = new HashMap<String, Counter>(size);
	}

	public void add(String key, int count) {
		Counter c = map.get(key);
		if (c == null) {
			c = new Counter(key);
			map.put(key, c);
		}
		c.count++;
	}

	public int size() {
		return map.size();
	}

	public Collection<Counter> values() {
		return map.values();
	}

	public void clear() {
		map.clear();
	}
}