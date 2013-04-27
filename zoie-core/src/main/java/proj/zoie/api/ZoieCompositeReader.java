package proj.zoie.api;

import java.io.IOException;

import org.apache.lucene.index.BaseCompositeReader;

public class ZoieCompositeReader extends BaseCompositeReader<ZoieSegmentReader> implements UIDMapper{

  private final ZoieSegmentReader[] subReaders;
  protected ZoieCompositeReader(ZoieSegmentReader[] subReaders) {
    super(subReaders);
    this.subReaders = subReaders;
  }

  @Override
  protected void doClose() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public long getUid(int docid) {
    int readerIdx = super.readerIndex(docid);
    int readerBase = super.readerBase(readerIdx);
    ZoieSegmentReader subreader = subReaders[readerIdx];
    return subreader.getUid(docid - readerBase);
  }

  @Override
  public int getDocid(long uid) {
    ZoieSegmentReader subreader;
    for (int i = subReaders.length-1; i>=0; --i) {
      subreader = subReaders[i];
      int docid = subreader.getDocid(uid);
      if (docid != DocIDMapper.NOT_FOUND) {
        return docid;
      }
    }
    return DocIDMapper.NOT_FOUND;
  }
}
