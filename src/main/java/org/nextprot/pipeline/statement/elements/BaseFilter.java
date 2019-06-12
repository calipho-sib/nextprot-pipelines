package org.nextprot.pipeline.statement.elements;



import org.nextprot.commons.statements.Statement;
import org.nextprot.pipeline.statement.Filter;
import org.nextprot.pipeline.statement.muxdemux.DuplicableElement;
import org.nextprot.pipeline.statement.ports.SinkPipePort;
import org.nextprot.pipeline.statement.ports.SourcePipePort;

import java.io.IOException;
import java.util.List;


public abstract class BaseFilter extends BasePipelineElement<DuplicableElement> implements Filter {

	protected BaseFilter(int capacity) {

		super(capacity, new SinkPipePort(capacity), new SourcePipePort(capacity));
	}

	@Override
	public boolean handleFlow(List<Statement> buffer) throws IOException {

		return filter(getSinkPipePort(), getSourcePipePort());
	}
}
