package org.nextprot.pipeline.statement.core.elements;

import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.core.PipelineElement;

import java.util.concurrent.BlockingQueue;

/**
 * A Source is a terminal pipeline element - it cannot pipe to another element
 *
 *    -----    ------
 * ==:  F  :==: Sink X
 *    -----    ------
 */
public abstract class Source extends BasePipelineElement<PipelineElement> {

	/** poisoned statement pill */
	public static final Statement POISONED_STATEMENT = new Statement();

	protected Source(int sourceCapacity) {

		super(sourceCapacity);
	}

	@Override
	public void setSinkChannel(BlockingQueue<Statement> sinkChannel) {

		throw new Error("Cannot set a pipeline element into a SOURCE");
	}

	@Override
	public BlockingQueue<Statement> getSinkChannel() {

		throw new Error("It is a SOURCE element, cannot pipe to a pipeline element through this channel!");
	}

	protected int countPoisonedPillsToProduce() {

		PipelineElement element = this;

		while ((element = element.nextSink()) != null) {

			if (element instanceof Demux) {
				return ((Demux)element).countSourceChannels();
			}
		}
		return 1;
	}
}
