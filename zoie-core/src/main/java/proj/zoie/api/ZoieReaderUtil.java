package proj.zoie.api;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.NumericDocValues;

import proj.zoie.api.indexing.AbstractZoieIndexable;

public class ZoieReaderUtil {
  
  public static NumericDocValues getUidValues(AtomicReader reader) throws IOException {
    return reader.getNumericDocValues(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD);
  }
  
}
