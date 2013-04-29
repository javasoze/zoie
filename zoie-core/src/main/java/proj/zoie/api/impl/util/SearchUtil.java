package proj.zoie.api.impl.util;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieSegmentReader;

public class SearchUtil
{
  private static final Logger log = Logger.getLogger(SearchUtil.class);

  /**
   * @param zoie
   * @param field
   * @param query
   * @return a String representation of the search result given the string representations of query arguments.
   */
  public static String search(Zoie zoie, String field, String query)
  {
    DecimalFormat formatter = new DecimalFormat("00000000000000000000");
    List<ZoieIndexReader<?>> readers = null;
    IndexSearcher searcher = null;
    QueryParser parser = null;
    parser = new QueryParser(Version.LUCENE_42, field, zoie.getAnalyzer());
    parser.setAllowLeadingWildcard(true);
    Query q = null;
    try
    {
      q = parser.parse(query);
    } 
    catch (ParseException e)
    {
      return "query parsing failed " + e;
    }
    if (q == null)
      return "query parsed into null";
    String retstr = q + "\n";
    String docstr = "";
    try
    {
      readers = zoie.getIndexReaders();
      retstr += readers.size() + " readers obtained\n";
      for (int readerid = 0; readerid < readers.size(); readerid++)
      {
        retstr += "reader: " + readerid + "\n";
        docstr += "reader: " + readerid + "\n";
        ZoieIndexReader reader = readers.get(readerid);
        
        DocIDMapper idmapper = reader.getDocIDMaper();
        
        
        searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(q, 10);
        String docs = "";
        for (int i = 0; i < hits.scoreDocs.length; i++)
        {
          int docid = hits.scoreDocs[i].doc;
          float score = hits.scoreDocs[i].score;
          Explanation exp = searcher.explain(q, docid);
          Document doc = reader.document(docid);
          long uid = reader.getUID(docid);
          docs = docs + "UID: " + formatter.format(uid) + "\ndocid(in reader): " + formatter.format(docid) + "\nscore: " + score + "\n\n";
          docstr = docstr + "UID: " + formatter.format(uid) + "\ndocid(in reader): " + formatter.format(docid) + "\nscore: " + score + "\n" + doc + "\n" + exp + "\n\n";
        }
        retstr += hits.totalHits + " hits returned\n" + docs + "\n";
      }
    } catch (IOException e)
    {
      log.error(e);
    } finally
    {
      zoie.returnIndexReaders(readers);
    }
    return retstr + docstr;
  }

  /**
   * @param zoie
   * @param UID
   * @return the a String representation of the Document(s) referred to by the given UID
   */
  public static String getDocument(Zoie zoie, long UID)
  {
    List<ZoieIndexReader<?>> readers = null;
    DecimalFormat formatter = new DecimalFormat("00000000000000000000");
    String retstr = "UID: " + formatter.format(UID) + "\n";
    try
    {
      readers = zoie.getIndexReaders();
      retstr += readers.size() + " readers obtained\n";
      for (int readerid = 0; readerid < readers.size(); readerid++)
      {
        retstr += "reader: " + readerid + "\n";
        ZoieIndexReader reader = readers.get(readerid);
        
        int docid = reader.getDocID(UID);
        retstr += "docid(in reader): " + formatter.format(docid) + "\n";
        if (docid == DocIDMapper.NOT_FOUND)
        {
          retstr += "not found in this reader\n";
          continue;
        }
        if (docid==ZoieSegmentReader.DELETED_UID || !reader.getLiveDocs().get(docid))
        {
          retstr += "deleted\n";
        }
        Document doc = reader.document(docid);
        retstr += doc + "\n";
      }
    } catch (IOException e)
    {
      log.error(e);
    } finally
    {
      zoie.returnIndexReaders(readers);
    }
    return retstr;
  }
}
