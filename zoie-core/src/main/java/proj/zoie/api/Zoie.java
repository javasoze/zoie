package proj.zoie.api;

import javax.management.StandardMBean;

import org.apache.lucene.index.AtomicReader;

import proj.zoie.mbean.ZoieAdminMBean;

public interface Zoie<R extends AtomicReader, D> extends DataConsumer<D>, IndexReaderFactory<ZoieIndexReader<R>>
{
  void start();
  void shutdown();
  StandardMBean getStandardMBean(String name);
  String[] getStandardMBeanNames();
  ZoieAdminMBean getAdminMBean();
  void syncWithVersion(long timeInMillis, String version) throws ZoieException;
  void flushEvents(long timeout) throws ZoieException;
}
