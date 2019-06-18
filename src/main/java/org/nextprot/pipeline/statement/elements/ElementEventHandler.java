package org.nextprot.pipeline.statement.elements;


public interface ElementEventHandler {

	void valvesOpened();
	void sinkUnpiped();
	void sourceUnpiped();
	void valvesClosed();
	void disconnected();

	class Mute implements ElementEventHandler {

		@Override
		public void valvesOpened() { }

		@Override
		public void sinkUnpiped() { }

		@Override
		public void sourceUnpiped() { }

		@Override
		public void valvesClosed() { }

		@Override
		public void disconnected() { }
	}
}
