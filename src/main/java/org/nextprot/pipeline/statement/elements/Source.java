package org.nextprot.pipeline.statement.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.PipelineElement;

import java.util.concurrent.BlockingQueue;

public abstract class Source extends BasePipelineElement<PipelineElement> {

	/** poisoned statement pill */
	public static final Statement POISONED_STATEMENT = new Statement();

	protected Source(int sourceCapacity) {

		super(sourceCapacity);
	}

	@Override
	public BlockingQueue<Statement> getSinkChannel() {

		throw new Error("It is a Source element, can't connect to a PipelineElement through this channel!");
	}

	protected int countPoisonedPillsToProduce() {

		PipelineElement element = this;

		while ((element = element.nextElement()) != null) {

			if (element instanceof Demux) {
				return ((Demux)element).countSourceChannels();
			}
		}
		return 1;
	}
}
