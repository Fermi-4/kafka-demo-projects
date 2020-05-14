package state;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Mock slot data obj to demo with
 * 
 * for example, 3 slots
 * 
 * @author colte
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SlotData {
	String tester;
	String slotId;
	String boardId;
	String loadDate;
}
