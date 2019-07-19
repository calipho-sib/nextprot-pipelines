package org.nextprot.pipeline.statement.core.stage;


public interface ElementEventHandler {

	void sinkPiped();
	void sinkUnpiped();
	void sourceUnpiped();

	class Mute implements ElementEventHandler {

		@Override
		public void sinkPiped() { }

		@Override
		public void sinkUnpiped() { }

		@Override
		public void sourceUnpiped() { }
	}
}
