package proj.zoie.impl.indexing.internal;

import org.apache.lucene.index.AtomicReader;

import proj.zoie.api.indexing.IndexReaderDecorator;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 * 
 */
public abstract class RAMIndexFactory<R extends AtomicReader>
{
  public abstract RAMSearchIndex<R> newInstance(String version, IndexReaderDecorator<R> decorator, SearchIndexManager<R> idxMgr);
}
