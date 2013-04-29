package proj.zoie.api;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.CompositeReader;

public interface DocIDMapperFactory {
  DocIDMapper getDocIDMapper(AtomicReader reader) throws IOException;
  DocIDMapper getDocIDMapper(CompositeReader reader) throws IOException;
}
