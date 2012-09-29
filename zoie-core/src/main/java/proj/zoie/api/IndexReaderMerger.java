package proj.zoie.api;

import java.util.List;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;

public interface IndexReaderMerger<R extends IndexReader,T extends AtomicReader> {
	R mergeIndexReaders(List<ZoieIndexReader<T>> readerList);
}
