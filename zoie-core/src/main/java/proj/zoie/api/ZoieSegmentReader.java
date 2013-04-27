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
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.BytesRef;

import proj.zoie.api.impl.util.ArrayDocIdSet;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class ZoieSegmentReader<R extends AtomicReader> extends FilterAtomicReader implements UIDMapper,Cloneable{
	public static final long DELETED_UID = Long.MIN_VALUE;
	
	private R _decoratedReader;
    private NumericDocValues _uidValues;
    private IntRBTreeSet _delDocIdSet = new IntRBTreeSet();
    private int[] _currentDelDocIds;

	protected int[] _delDocIds;
	protected final IndexReaderDecorator<R> _decorator;
	
	long _minUID;
	long _maxUID;
	protected boolean _noDedup = false;

	private final DocIDMapper<?> _docIDMapper;
	
	public static void fillDocumentID(Document doc,long id){
	  Field uidField = new NumericDocValuesField(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD, id);
	  doc.add(uidField); 
	}

	public ZoieSegmentReader(AtomicReader in,
			DocIDMapper docIDMapper,
			IndexReaderDecorator<R> decorator)
			throws IOException {
		super(in);
		_docIDMapper = docIDMapper;
		_decorator = decorator;
		if (!(in instanceof SegmentReader)){
			throw new IllegalStateException("ZoieSegmentReader can only be constucted from "+SegmentReader.class);
		}
		init(in);
		_decoratedReader = (decorator == null ? null : decorator.decorate(this));
	}
	
  /**
   * make exact shallow copy for duplication. The decorated reader is also shallow copied.
   * @param copyFrom
   * @param innerReader
   * @throws IOException
   */
  ZoieSegmentReader(ZoieSegmentReader<R> copyFrom) throws IOException
  {
    this(copyFrom,copyFrom._delDocIds !=null && copyFrom._delDocIds.length>0);
  }
  
  ZoieSegmentReader(ZoieSegmentReader<R> copyFrom,boolean withDeletes) throws IOException
  {
    super(copyFrom.in);
    // we are creating another wrapper around the same inner reader, need to up the ref
    in.incRef();
    _decorator = copyFrom._decorator;
    _uidValues = copyFrom._uidValues;
    _maxUID = copyFrom._maxUID;
    _minUID = copyFrom._minUID;
    _noDedup = copyFrom._noDedup;
    _docIDMapper = copyFrom._docIDMapper;
    _delDocIdSet = copyFrom._delDocIdSet;
    _currentDelDocIds = copyFrom._currentDelDocIds;

    if (copyFrom._decorator == null)
    {
      _decoratedReader = null;
    } else
    {
      _decoratedReader = copyFrom._decorator.redecorate(copyFrom._decoratedReader, this, withDeletes);
    }
  }
  
    public DocIDMapper<?> getDocIDMaper(){
		return _docIDMapper;
	}
	
	public void markDeletes(LongSet delDocs, LongSet deletedUIDs)
	{
      DocIDMapper<?> idMapper = getDocIDMaper();
      LongIterator iter = delDocs.iterator();
      IntRBTreeSet delDocIdSet = _delDocIdSet;

      while(iter.hasNext())
      {
        long uid = iter.nextLong();
        if (DELETED_UID != uid)
        {
          int docid = idMapper.getDocID(uid);
          if(docid != DocIDMapper.NOT_FOUND)
          {
            delDocIdSet.add(docid);
            deletedUIDs.add(uid);
          }
        }
      }	  
	}
	
	public void commitDeletes()
	{
	  _currentDelDocIds = _delDocIdSet.toIntArray();
	}
	
	public void setDelDocIds()
	{
	  _delDocIds = _currentDelDocIds;
	  if (_decorator!=null && _decoratedReader!=null)
	    _decorator.setDeleteSet(_decoratedReader, new ArrayDocIdSet(_currentDelDocIds));
	}
	
	public R getDecoratedReader()
    {
	  return _decoratedReader;
    }
	
    public BytesRef getStoredValue(long uid) throws IOException {
      int docid = this.getDocIDMaper().getDocID(uid);
      if (docid<0) return null;
    
      if (docid>=0){
        Document doc = document(docid);
        if (doc!=null){
          return doc.getBinaryValue(AbstractZoieIndexable.DOCUMENT_STORE_FIELD);
        }
      }
      return null;
    }
	
	private void init(AtomicReader reader) throws IOException
	{
	  _uidValues = reader.getNumericDocValues(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD);
	}
	
	@Override
	public long getUid(int docid)
	{
	  return _uidValues.get(docid);
	}

	public NumericDocValues getUIDValues()
	{
		return _uidValues;
	}
	
	public String getSegmentName(){
		return ((SegmentReader)in).getSegmentName();
	}

	@Override
	protected synchronized void doClose() throws IOException {
		_decoratedReader.close();
	}

   @Override
   public int numDocs() {
     if (_currentDelDocIds != null) {
       return super.maxDoc() - _currentDelDocIds.length;
     }  else {
       return super.numDocs();
     }
   }
   
  /**
   * makes exact shallow copy of a given ZoieMultiReader
   * @param <R>
   * @param source
   * @return
   * @throws IOException
   */
  @Override
  public ZoieSegmentReader<R> clone()
  {
	try{
      return new ZoieSegmentReader<R>(this);
	}
	catch(IOException e){
		throw new IllegalStateException("problem cloning "+ZoieSegmentReader.class,e);
	}
  }

  @Override
  public int getDocid(long uid) {
	  return _docIDMapper.getDocID(uid);
  }
  
  private AtomicLong zoieRefSegmentCounter = new AtomicLong(1);
  
  public void incSegmentRef() {
    zoieRefSegmentCounter.incrementAndGet();
  }

  public void decSegmentRef() {
    long refCount = zoieRefSegmentCounter.decrementAndGet();
    if (refCount < 0) {
      throw new IllegalStateException("The segment ref count shouldn't be less than zero");
    }
    if (refCount == 0) {
      try {
        doClose();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
 
}
