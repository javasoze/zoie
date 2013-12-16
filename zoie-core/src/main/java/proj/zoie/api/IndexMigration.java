package proj.zoie.api;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

import proj.zoie.api.indexing.AbstractZoieIndexable;

public class IndexMigration {
  
  private static long bytesToLong(BytesRef bytesRef){
    int offSet = bytesRef.offset;
    byte[] bytes = bytesRef.bytes;
    return ((long)(bytes[offSet + 7] & 0xFF) << 56) | 
        ((long)(bytes[offSet + 6] & 0xFF) << 48) | 
        ((long)(bytes[5 + offSet] & 0xFF) << 40) | 
        ((long)(bytes[4 + offSet] & 0xFF) << 32) | 
        ((long)(bytes[3 + offSet] & 0xFF) << 24) | 
        ((long)(bytes[2 + offSet] & 0xFF) << 16)
       | ((long)(bytes[1 + offSet] & 0xFF) <<  8) |  
       (long)(bytes[0 + offSet] & 0xFF);
  }

  static AtomicReader decorated(AtomicReader in) {
    
 // read payload data:
    int maxDoc = in.maxDoc();
    final long[] uidArray = new long[maxDoc];
    try {
      Terms terms = in.fields().terms(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD);
      TermsEnum te = terms.iterator(null);
      if (!te.seekExact(new BytesRef("_UID"), false)) {
        throw new RuntimeException("term not found");
      }
      DocsAndPositionsEnum de = te.docsAndPositions(null, null);
      int docid;
      while ((docid = de.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        int numFreq = de.freq();
        assert numFreq == 1;
        for (int i = 0; i < numFreq; ++i) {
          de.nextPosition();
          BytesRef payload = de.getPayload();
          uidArray[docid] = bytesToLong(payload);
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    
    return new FilterAtomicReader(in) {
      
      
      
      @Override
      public Fields fields() throws IOException {
        Fields f =  super.fields();
        return new FilterFields(f) {

          @Override
          public Terms terms(String field) throws IOException {
            if (AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD.equals(field)) 
              return null;
            return super.terms(field);
          }
          
        };
      }

      @Override
      public FieldInfos getFieldInfos() {
        FieldInfos infos = super.getFieldInfos();
        int size = infos.size();        
        FieldInfo newField = new FieldInfo(
            AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD, 
            false,
            size,
            false,
            false,
            false,
            IndexOptions.DOCS_ONLY,
            DocValuesType.NUMERIC,
            DocValuesType.NUMERIC,
            null
            );
        ArrayList<FieldInfo> infosList = new ArrayList<FieldInfo>();
        for (FieldInfo info : infos) {
          if (!AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD.equals(info.name)) {
            infosList.add(info);
          }
        }
        infosList.add(newField);
        
        return new FieldInfos(infosList.toArray(new FieldInfo[0]));
      }

      @Override
      public NumericDocValues getNumericDocValues(String field)
          throws IOException {
        NumericDocValues docvals = super.getNumericDocValues(field);
        if (docvals == null && (AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD.equals(field))) {
          return new NumericDocValues() {
            
            @Override
            public long get(int docID) {
              return uidArray[docID];
            }
          };
        }
        return docvals;
      }
      
    };
  }
  
  static boolean openZoieReader(File srcIdx) throws Exception {
    DirectoryReader reader = DirectoryReader.open(FSDirectory.open(srcIdx));
    ZoieMultiReader zoieReader = null;
    try {
      zoieReader = new ZoieMultiReader(reader, null);
      return true;
    }
    catch (Exception e) {
      return false;
    }
    finally {
      if (zoieReader != null) {
        zoieReader.close(); 
      }
    }
  }
  
	public static void main(String[] args) throws Exception {
	  File output = new File("/tmp/zoie-migrated");
	  
	  File srcIdx = new File(args[0]);
	  
	  if (openZoieReader(srcIdx)) {
	    return;
	  }
	  
	  DirectoryReader reader = DirectoryReader.open(FSDirectory.open(srcIdx));
	  
	  List<AtomicReader> atomicReaderList = new ArrayList<AtomicReader>();
	  for (AtomicReaderContext ctx : reader.leaves()) {
	    atomicReaderList.add(decorated(ctx.reader()));
	  }
	  
	  
	  IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, null);
	  
	  IndexWriter writer = new IndexWriter(FSDirectory.open(output), conf);
	  
	  writer.addIndexes(atomicReaderList.toArray(new AtomicReader[0]));
	  writer.commit();
	  writer.close();
	  
	  reader.close();
	  
	  // verify
	  boolean verified = openZoieReader(output);
	  System.out.println("verified: " + verified);
	}

}
