package org.nextprot.pipeline.statement;

import org.nextprot.pipeline.statement.ports.PipedInputPort;

import java.io.IOException;
import java.util.List;

public interface Pipe {

	void openPipe(List<Thread> collector);
	void closePipe() throws IOException;
	void connect(Pipe receiver) throws IOException;
	PipedInputPort getInputPort();
	int getCapacity();
}
