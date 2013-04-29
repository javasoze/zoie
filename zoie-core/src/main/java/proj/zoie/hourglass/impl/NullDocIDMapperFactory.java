package proj.zoie.hourglass.impl;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.CompositeReader;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;

public class NullDocIDMapperFactory implements DocIDMapperFactory
{
  public static final NullDocIDMapperFactory INSTANCE = new NullDocIDMapperFactory();
  
  @Override
  public DocIDMapper getDocIDMapper(AtomicReader reader) throws IOException {
    return NullDocIDMapper.INSTANCE;
  }
  @Override
  public DocIDMapper getDocIDMapper(CompositeReader reader) throws IOException {
    return NullDocIDMapper.INSTANCE;
  }  
}
