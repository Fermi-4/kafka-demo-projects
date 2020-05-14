package state.producer;

import java.time.LocalDateTime;
import java.util.function.Supplier;

import state.SlotData;

public class SlotDataSupplier implements Supplier<SlotData> {

	private static final String testerId = "tester1";
	
	private static final String boardId = "board1";
	
	private static final String slotId = "slot1";
	
	@Override
	public SlotData get() {
		
		boolean addBoard = false;
		if(Math.random() > 0.50) {
			addBoard = true;
		}
		
		SlotData slotdata;
		if(addBoard) {
			slotdata = new SlotData(testerId, slotId, boardId, LocalDateTime.now().toString());			
		} else {
			slotdata = new SlotData(testerId, slotId, "", LocalDateTime.now().toString());
		}
		
		return slotdata;
	}

}
