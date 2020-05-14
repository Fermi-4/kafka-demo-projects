package state;

import java.util.Collection;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import state.BoardEvent.BoardEventType;

public class SlotDataEventTransformer implements ValueTransformer<SlotData, BoardEvent> {

	private KeyValueStore<String, SlotData> stateStore;
	private final String storeName = "slot-data-state";
	private ProcessorContext context;

	@SuppressWarnings("unchecked")
	public void init(ProcessorContext context) {
		this.context = context;
		this.stateStore = (KeyValueStore<String, SlotData>) this.context.getStateStore(storeName);
	}

	public BoardEvent transform(SlotData value) {
		return BoardEventFactory.getSlotEvents(value, stateStore);
	}

	public void close() {

	}

	static class BoardEventFactory {

		static BoardEvent getSlotEvents(SlotData current, KeyValueStore<String, SlotData> stateStore) {

			if (current == null) {
				throw new RuntimeException("Value cannot be null! Cannot process data!");
			}

			SlotData prev = stateStore.get(current.getSlotId());

			/**
			 * this is greatly simplified for demo
			 */

			if (prev != null) {		
			
				// REMOVE
				if (current.getBoardId() == "") {
					if (prev.getBoardId() != current.getBoardId()) {
						stateStore.put(prev.getSlotId(), current);
						return new BoardEvent(prev.getBoardId(), current.getLoadDate(), BoardEventType.REMOVE);
					}
				}
				// ADD
				else if (prev.getBoardId() == "") {
					if (current.getBoardId() != prev.getBoardId()) {
						stateStore.put(current.getSlotId(), current);
						return new BoardEvent(current.getBoardId(), current.getLoadDate(), BoardEventType.ADD);
					}
				} else {
					return null;
				}
				// INIT
			} else if (prev == null && current.getBoardId() != "") {
				stateStore.put(current.getSlotId(), current);
				return new BoardEvent(current.getBoardId(), current.getLoadDate(), BoardEventType.INIT);
			}
			
			return null;

		}

	}

	/**
	 * chain where the order matters
	 */
	interface BoardChurnSlotDataProcessor {
		Collection<BoardEvent> process(SlotData current, SlotData prev, Collection<BoardEvent> events);
	}

	class BoardChurnProcessorChain {

		BoardChurnProcessorLink chain;

		public BoardChurnProcessorChain(BoardChurnProcessorLink chain) {
			super();
			this.chain = chain;
		}

	}

	abstract class BoardChurnProcessorLink implements BoardChurnSlotDataProcessor {

		BoardChurnSlotDataProcessor next;

		public BoardChurnProcessorLink(BoardChurnSlotDataProcessor next) {
			super();
			this.next = next;
		}

	}

}
