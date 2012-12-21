package example.entity;

import jp.co.cac.azarea.cluster.entity.DefaultEntity;
import jp.co.cac.azarea.cluster.entity.EntitySchema;
import jp.co.cac.azarea.cluster.entity.GeneralEntitySchema;
import jp.co.cac.azarea.cluster.entity.column.EntityColumn;
import jp.co.cac.azarea.cluster.entity.column.IntEntityColumn;
import jp.co.cac.azarea.cluster.entity.column.StringEntityColumn;
import jp.co.cac.azarea.cluster.util.Generated;

/**
 *
 */
@Generated("AZAREA-Cluster 1.0")
public class WordCountEntity extends DefaultEntity {
	
	/**
	 *
	 */
	private static final EntityColumn wordColumn = new StringEntityColumn("word");
	/**
	 *
	 */
	private static final EntityColumn countColumn = new IntEntityColumn("count", null);
	
	private static final EntityColumn[] ALL_COLUMNS = {
		wordColumn,
		countColumn
	};
	
	private static final EntityColumn[] KEY_COLUMNS = {
		
	};
	
	private static final EntitySchema schema = new GeneralEntitySchema("WordCountEntity", ALL_COLUMNS, KEY_COLUMNS);
	
	/**
	 *
	 */
	public String word;
	
	/**
	 *
	 */
	public int count;
	
	
	public WordCountEntity() {
		super(schema);
	}
	
	/**
	 * エンティティを初期化する。
	 */
	@Override
	protected void initialize() {
		
		
		super.initialize();
	}
	
	
	@Override
	protected Object innerGetItem(int index) {
		switch(index) {
			case 0:
			return this.word;
			
			case 1:
			return this.count;
			
			default:
			throw new IllegalArgumentException("Illegal index : " + index);
		}
	}
	
	@Override
	protected void innerSetItem(int index, Object value) {
		switch(index) {
			case 0:
			this.word = (String)value;
			break;
			
			case 1:
			this.count = (Integer)value;
			break;
			
			default:
			throw new IllegalArgumentException("Illegal index : " + index);
		}
	}
	
}
