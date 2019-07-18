package org.nextprot.pipeline.statement.core.elements;


public interface ElementEventHandler {

	void valveOpened();
	void sinkPiped();
	void sinkUnpiped();
	void sourceUnpiped();
	void valveClosed();

	class Mute implements ElementEventHandler {

		@Override
		public void valveOpened() { }

		@Override
		public void sinkPiped() { }

		@Override
		public void sinkUnpiped() { }

		@Override
		public void sourceUnpiped() { }

		@Override
		public void valveClosed() { }
	}
}
