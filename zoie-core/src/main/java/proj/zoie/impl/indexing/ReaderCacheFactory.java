package proj.zoie.impl.indexing;

import org.apache.lucene.index.AtomicReader;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

public interface ReaderCacheFactory
{
  public <R extends AtomicReader> AbstractReaderCache<R> newInstance(IndexReaderFactory<ZoieIndexReader<R>> readerfactory);
}
