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
import java.io.IOException;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;

/**
 * Filter implementation based on a list of uids
 */
public class UIDFilter extends Filter {
	private final long[] _filteredIDs;

	public UIDFilter(long[] filteredIDs) {
		_filteredIDs = filteredIDs;
	}

	@Override
	public DocIdSet getDocIdSet(AtomicReaderContext ctx,Bits bits) throws IOException {
		AtomicReader reader = ctx.reader();
		if (reader instanceof ZoieSegmentReader<?>) {
			return new UIDDocIdSet(_filteredIDs, ((ZoieSegmentReader<?>)reader).getDocIDMaper());
		}
		else {
			throw new IllegalArgumentException(
			"UIDFilter may only load from "+ZoieSegmentReader.class+" instances");
		}
	}
	
	/**
	 * Convenience method to build a Query from a uid list
	 * @param uids
	 * @return
	 */
	public static Query getUIDQuery(long[] uids){
		return new ConstantScoreQuery(new UIDFilter(uids));
	}
}
