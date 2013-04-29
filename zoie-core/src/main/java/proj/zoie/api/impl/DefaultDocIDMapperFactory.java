package proj.zoie.api.impl;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CompositeReader;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieReaderUtil;

public class DefaultDocIDMapperFactory implements DocIDMapperFactory {
  
  @Override
	public DocIDMapper getDocIDMapper(AtomicReader reader) throws IOException {
		return new DocIDMapperImpl(ZoieReaderUtil.getUidValues(reader), reader.maxDoc());
	}

  @Override
  public DocIDMapper getDocIDMapper(final CompositeReader reader) throws IOException{
    final AtomicReaderContext[] subReaderContext = reader.leaves().toArray(new AtomicReaderContext[0]);
    final DocIDMapper[] mappers = new DocIDMapper[subReaderContext.length];
    for (int i = 0; i < subReaderContext.length; ++i) {
      mappers[i] = getDocIDMapper(subReaderContext[i].reader());
    }
    
    return new DocIDMapper() {
      
      @Override
      public int quickGetDocID(long uid) {
        int docid;
        for (int i = mappers.length-1; i >= 0; --i) {
          docid = mappers[i].getDocID(uid);
          if (docid != DocIDMapper.NOT_FOUND) {
            return docid;
          }
        }
        return DocIDMapper.NOT_FOUND;
      }
      
      @Override
      public int getDocID(long uid) {
        return quickGetDocID(uid);
      }
    };
  }
}
