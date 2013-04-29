package proj.zoie.api;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BaseCompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.BytesRef;

import proj.zoie.api.indexing.IndexReaderDecorator;

public class ZoieIndexReader<R extends AtomicReader> extends BaseCompositeReader<ZoieSegmentReader<R>> implements UIDMapper,Cloneable
{
  private static final Logger log = Logger.getLogger(ZoieIndexReader.class.getName());
	private Map<String,ZoieSegmentReader<R>> _readerMap;
	private ZoieSegmentReader<R>[] _subZoieReaders;
	private List<R> _decoratedReaders;
	private final IndexReaderDecorator<R> _decorator;
	private final boolean closeSubReaders;
	private final DirectoryReader innerReader;
	private final DocIDMapper docidMapper;
	
	public ZoieIndexReader(DirectoryReader innerReader,
			ZoieSegmentReader<R>[] readers,IndexReaderDecorator<R> decorator,
			DocIDMapper docidMapper,
			boolean closeSubReaders)
	{
	  super(readers);
	  this.docidMapper = docidMapper;
	  this.closeSubReaders = closeSubReaders;
	  this.innerReader = innerReader;
	  _decorator = decorator;
	  _readerMap = new HashMap<String,ZoieSegmentReader<R>>();
	  _decoratedReaders = null; 
	  init(readers);
	}
	
	public BytesRef getStoredValue(long uid) throws IOException {
      int docid = docidMapper.getDocID(uid);
      if (docid < 0) return null;
      int idx = readerIndex(docid);
      if (idx < 0) return null;
      ZoieSegmentReader<R> subReader = _subZoieReaders[idx];
      return subReader.getStoredValue(uid);
	}
	
	private void init(ZoieSegmentReader<R>[] subReaders){
		_subZoieReaders = subReaders;
		
		for (ZoieSegmentReader<R> zr : subReaders){
			String segmentName = zr.getSegmentName();
			// zr.getVersion();
			_readerMap.put(segmentName, zr);
			if (!closeSubReaders) {
				zr.incRef();
			}
		}
		
		
		ArrayList<R> decoratedList = new ArrayList<R>(_subZoieReaders.length);
    	for (ZoieSegmentReader<R> subReader : _subZoieReaders){
    		R decoratedReader = subReader.getDecoratedReader();
    		decoratedList.add(decoratedReader);
    	}
    	_decoratedReaders = decoratedList;
	}
	
	@Override
	public long getUid(int docid)
	{
		int idx = readerIndex(docid);
		int docBase = super.readerBase(idx);
		ZoieSegmentReader<R> subReader = _subZoieReaders[idx];
		return subReader.getUid(docid- docBase);
	}
	
	public long getMinUID()
    {
      long uid = Long.MAX_VALUE;
      for(ZoieSegmentReader<R> reader : _subZoieReaders)
      {
        uid = Math.min(uid, reader._minUID);
      }
      return uid;
    }

    public long getMaxUID()
    {
      long uid = Long.MIN_VALUE;
      for(ZoieSegmentReader<R> reader : _subZoieReaders)
      {
        uid = Math.max(uid, reader._maxUID);
      }
      return uid;
    }
	
	public void markDeletes(LongSet delDocs, LongSet deletedUIDs)
	{
	  for(ZoieSegmentReader<R> subReader : _subZoieReaders) {
	    subReader.markDeletes(delDocs, deletedUIDs);
    } 
	}
	
	public void commitDeletes()
	{
		for(ZoieSegmentReader<R> subReader : _subZoieReaders)
	    {
	      subReader.commitDeletes();
	    }
	}
	
	public void setDelDocIds()
	{
	  for(ZoieSegmentReader<R> subReader : _subZoieReaders)
	  {
	    subReader.setDelDocIds();
	  }
	}

	public List<R> getDecoratedReaders() throws IOException{
	      return _decoratedReaders;
	}
	
	protected boolean hasIndexDeletions(){
		for (ZoieSegmentReader<R> subReader : _subZoieReaders){
			return subReader.getLiveDocs() != null;
		}
		return false;
	}
	
	public boolean isDeleted(int docid){
	   int idx = readerIndex(docid);
	   int docBase = readerBase(idx);
	   ZoieSegmentReader<R> subReader = _subZoieReaders[idx];
	   return subReader.getLiveDocs().get(docid-docBase);
	}
	
	
	@Override
	protected void doClose() throws IOException {
	  for (ZoieSegmentReader<R> r : _subZoieReaders){
		  if (closeSubReaders){
			r.close();
		  }
		  else{
	        r.decRef();
		  }
	  }
	}
	
  public static <R extends AtomicReader> ZoieIndexReader<R> openIfChanged(ZoieIndexReader<R> reader) throws IOException{
      long version = reader.innerReader.getVersion();
      
      DirectoryReader inner = DirectoryReader.openIfChanged(reader.innerReader);
	  if (inner == reader.innerReader && inner.getVersion()==version){
	    return reader;
	  }
	  
	  List<AtomicReaderContext> subReaderCtx = inner.leaves();
	  
	  ArrayList<ZoieSegmentReader<R>> subReaderList = new ArrayList<ZoieSegmentReader<R>>(subReaderCtx.size());
	  for (AtomicReaderContext ctx : subReaderCtx){
	    AtomicReader subReader = ctx.reader();
		  if (subReader instanceof SegmentReader){
			  SegmentReader sr = (SegmentReader)subReader;
			  String segmentName = sr.getSegmentName();
				ZoieSegmentReader<R> zoieSegmentReader = reader._readerMap.get(segmentName);
				if (zoieSegmentReader!=null){
					int numDocs = sr.numDocs();
					int maxDocs = sr.maxDoc();
					boolean hasDeletes = false;
					if (zoieSegmentReader.numDocs() != numDocs || zoieSegmentReader.maxDoc() != maxDocs){
						hasDeletes = true;
					}
					zoieSegmentReader = new ZoieSegmentReader<R>(zoieSegmentReader,hasDeletes);
				}
				else{
					zoieSegmentReader = new ZoieSegmentReader<R>(sr,reader.docidMapper,reader._decorator);
				}
				subReaderList.add(zoieSegmentReader);
		  }
		  else{
			throw new IllegalStateException("reader not insance of "+SegmentReader.class);
		  }
	  }
	  
	  ZoieIndexReader<R> ret = new ZoieIndexReader<R>(reader.innerReader,
			  subReaderList.toArray(new ZoieSegmentReader[subReaderList.size()]) ,
			  reader._decorator, reader.docidMapper, reader.closeSubReaders);
	  return ret;
  }
   
  /**
   * makes exact shallow copy of a given ZoieMultiReader
   * @param <R>
   * @param source
   * @return
   * @throws IOException
   */
  @Override
  public ZoieIndexReader<R> clone()
  {
    ZoieSegmentReader<R>[] sourceZoieSubReaders = this._subZoieReaders;
    ArrayList<ZoieSegmentReader<R>> zoieSubReaders = new ArrayList<ZoieSegmentReader<R>>(this._subZoieReaders.length);
    for(ZoieSegmentReader<R> r : sourceZoieSubReaders)
    {
      zoieSubReaders.add(r.clone());
    }
     
    ZoieIndexReader<R> ret = new ZoieIndexReader<R>(this.innerReader,
    		zoieSubReaders.toArray(new ZoieSegmentReader[zoieSubReaders.size()])
    		,_decorator, this.docidMapper, this.closeSubReaders);
    return ret;
  }

  @Override
  public int getDocid(long uid) {
	return docidMapper.getDocID(uid);
  }
}
