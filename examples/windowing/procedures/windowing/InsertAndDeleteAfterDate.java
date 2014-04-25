/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package windowing;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.types.TimestampType;

public class InsertAndDeleteAfterDate extends VoltProcedure {

    final SQLStmt insert = new SQLStmt(
            "INSERT INTO timedata (uuid, val, update_ts) VALUES (?, ?, ?);");

    final SQLStmt countMatchingRows = new SQLStmt(
            "SELECT COUNT(*) FROM timedata WHERE update_ts <= ?;");

    final SQLStmt getNthOldestTimestamp = new SQLStmt(
            "SELECT update_ts FROM timedata ORDER BY update_ts ASC OFFSET ? LIMIT 1;");

    final SQLStmt deleteOlderThanDate = new SQLStmt(
            "DELETE FROM timedata WHERE update_ts <= ?;");

    /**
     *
     * @param uuid Column value for tuple insertion and partitioning key for this procedure.
     * @param val Column value for tuple insertion.
     * @param update_ts Column value for tuple insertion.
     * @param newestToDiscard Try to remove any tuples as old or older than this value.
     * @param maxRowsToDeletePerProc The upper limit on the number of rows to delete per transaction.
     * @return The number of deleted rows.
     * @throws VoltAbortException on bad input.
    */
    public long run(String uuid, long val, TimestampType update_ts, TimestampType newestToDiscard, long maxRowsToDeletePerProc) {
        if (newestToDiscard == null) {
            throw new VoltAbortException("newestToDiscard shouldn't be null.");
            // It might be Long.MIN_VALUE as a TimestampType though.
        }
        if (maxRowsToDeletePerProc <= 0) {
            throw new VoltAbortException("maxRowsToDeletePerProc must be > 0.");
        }

        // This line inserts the row.
        voltQueueSQL(insert, EXPECT_SCALAR_MATCH(1), uuid, val, update_ts);
        // In the same round trip to the storage engine, count the rows.
        voltQueueSQL(countMatchingRows, EXPECT_SCALAR_LONG, newestToDiscard);
        // Can assume insert worked because of EXPECT_SCALAR_MATCH(1)
        // Note that the index into the set of results tables below is the second table.
        long agedOutCount = voltExecuteSQL()[1].asScalarLong();

        if (agedOutCount > maxRowsToDeletePerProc) {
            // Find the timestamp of the row at position N in the sorter order, where N is the chunk size
            voltQueueSQL(getNthOldestTimestamp, EXPECT_SCALAR, maxRowsToDeletePerProc);
            newestToDiscard = voltExecuteSQL()[0].fetchRow(0).getTimestampAsTimestamp(0);
        }

        // Delete all rows >= the timestamp found in the previous statement.
        // This will delete AT LEAST N rows, but since timestamps may be non-unique,
        //  it might delete more than N. In the worst case, it could delete all rows
        //  if every row has an indentical timestamp value. It is guaranteed to make
        //  progress. If we used strictly less than, it might not make progress.
        voltQueueSQL(deleteOlderThanDate, EXPECT_SCALAR_LONG, newestToDiscard);
        long deletedCount = voltExecuteSQL(true)[0].asScalarLong();

        return deletedCount;
    }
}
