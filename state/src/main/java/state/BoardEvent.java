package state;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BoardEvent {
	public static enum BoardEventType {
		ADD,
		REMOVE,
		INIT
	}
	
	String boardId;
	String date;
	BoardEventType type;

}
