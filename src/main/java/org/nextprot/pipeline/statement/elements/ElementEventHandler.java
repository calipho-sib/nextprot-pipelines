package org.nextprot.pipeline.statement.elements;


public interface ElementEventHandler {

	void valvesOpened();
	void sinkPiped();
	void sinkUnpiped();
	void sourceUnpiped();
	void valvesClosed();

	class Mute implements ElementEventHandler {

		@Override
		public void valvesOpened() { }

		@Override
		public void sinkPiped() { }

		@Override
		public void sinkUnpiped() { }

		@Override
		public void sourceUnpiped() { }

		@Override
		public void valvesClosed() { }
	}
}
