package org.nextprot.pipeline.statement.elements;


public interface ElementEventHandler {

	void sinkPipePortUnpiped();
	void sourcePipePortUnpiped();
	void elementClosed();

	class Mute implements ElementEventHandler {

		@Override
		public void sinkPipePortUnpiped() { }

		@Override
		public void sourcePipePortUnpiped() { }

		@Override
		public void elementClosed() { }
	}
}
