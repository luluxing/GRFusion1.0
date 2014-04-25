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
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

public class DeleteOldestToTarget extends VoltProcedure {

    public final SQLStmt countRows = new SQLStmt(
            "SELECT COUNT(*) FROM timedata;");

    public final SQLStmt getNthOldestTimestamp = new SQLStmt(
            "SELECT update_ts FROM timedata ORDER BY update_ts ASC OFFSET ? LIMIT 1;");

    public final SQLStmt deleteAll = new SQLStmt(
            "DELETE FROM timedata;");

    public final SQLStmt deleteOlderThanDate = new SQLStmt(
            "DELETE FROM timedata WHERE update_ts <= ?;");

    final VoltTable retvalTemplate = new VoltTable(
            new VoltTable.ColumnInfo("deleted", VoltType.BIGINT),
            new VoltTable.ColumnInfo("not_deleted", VoltType.BIGINT));

    /**
     *
     * @param partitionValue Partitioning key for this procedure.
     * @param maxTotalRows The desired number of rows per partition.
     * @param maxRowsToDeletePerProc The upper limit on the number of rows to delete per transaction.
     * @return A table with one row containing the number of deleted rows and the number that could
     * have been deleted (but weren't) if there were no per-transaction delete limits.
     * @throws VoltAbortException on bad input.
     */
    public VoltTable run(String partitionValue, long maxTotalRows, long maxRowsToDeletePerProc) {
        if (maxRowsToDeletePerProc <= 0) {
            throw new VoltAbortException("maxRowsToDeletePerProc must be > 0.");
        }
        if (maxTotalRows < 0) {
            throw new VoltAbortException("maxTotalRows must be >= 0.");
        }

        // Count the rows in the current partition.
        voltQueueSQL(countRows, EXPECT_SCALAR_LONG);
        long count = voltExecuteSQL()[0].asScalarLong();

        // Return a table containing the number of rows deleted and the number
        // of rows that could have been deleted (but weren't) if there were no
        // per-transaction delete limits.
        // VoltTable.clone(int) copies the schema of a table but not the data.
        VoltTable retval = retvalTemplate.clone(20); // 20b is plenty to hold two longs

        // If partition is smaller than desired, return
        if (count < maxTotalRows) {
            retval.addRow(0, 0);
            return retval;
        }

        // If asked to remove all rows, go ahead
        if ((maxTotalRows == 0) && (count < maxRowsToDeletePerProc)) {
            voltQueueSQL(deleteAll, EXPECT_SCALAR_MATCH(count));
            voltExecuteSQL(true);
            retval.addRow(count, 0);
            return retval;
        }

        // Figure out how many rows to try to delete.
        long agedOutCount = count - maxTotalRows;
        long rowsToConsider = Math.min(agedOutCount, maxRowsToDeletePerProc);

        // Find the timestamp of the row at position N in the sorter order, where N is the chunk size
        voltQueueSQL(getNthOldestTimestamp, EXPECT_SCALAR, rowsToConsider);
        TimestampType newestToDiscard = voltExecuteSQL()[0].fetchRow(0).getTimestampAsTimestamp(0);

        // Delete all rows >= the timestamp found in the previous statement.
        // This will delete AT LEAST N rows, but since timestamps may be non-unique,
        //  it might delete more than N. In the worst case, it could delete all rows
        //  if every row has an indentical timestamp value. It is guaranteed to make
        //  progress. If we used strictly less than, it might not make progress.
        voltQueueSQL(deleteOlderThanDate, EXPECT_SCALAR_LONG, newestToDiscard);
        long deletedCount = voltExecuteSQL(true)[0].asScalarLong();

        retval.addRow(deletedCount, agedOutCount - deletedCount);
        return retval;
    }
}
