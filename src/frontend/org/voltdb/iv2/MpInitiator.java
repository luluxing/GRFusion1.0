/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.util.concurrent.CountDownLatch;

import java.util.List;

import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.voltcore.messaging.HostMessenger;

import org.voltcore.zk.MapCache;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.VoltZK;

/**
 * Subclass of Initiator to manage multi-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public class MpInitiator extends BaseInitiator
{
    private static final int MP_INIT_PID = -1;

    public MpInitiator(HostMessenger messenger, long buddyHSId)
    {
        super(VoltZK.iv2mpi,
                messenger,
                MP_INIT_PID,
                new MpScheduler(
                    buddyHSId,
                    new SiteTaskerQueue(),
                    new MapCache(messenger.getZK(), VoltZK.iv2masters)),
                "MP");
    }

    @Override
    public void configure(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          int kfactor, CatalogSpecificPlanner csp,
                          int numberOfPartitions,
                          boolean createForRejoin)
    {
        super.configureCommon(backend, serializedCatalog, catalogContext,
                numberOfPartitions, csp, numberOfPartitions,
                createForRejoin && isRejoinable());
    }

    /**
     * The MPInitiator does not have user data to rejoin.
     */
    @Override
    public boolean isRejoinable()
    {
        return false;
    }

    @Override
    public Term createTerm(CountDownLatch missingStartupSites, ZooKeeper zk,
            int partitionId, long initiatorHSId, InitiatorMailbox mailbox,
            String zkMapCacheNode, String whoami)
    {
        return new MpTerm(missingStartupSites, zk, initiatorHSId, mailbox, whoami);
    }

    @Override
    public RepairAlgo createPromoteAlgo(List<Long> survivors, ZooKeeper zk,
            int partitionId, InitiatorMailbox mailbox,
            String zkMapCacheNode, String whoami)
    {
        return new MpPromoteAlgo(m_term.getInterestingHSIds(), m_messenger.getZK(),
                m_partitionId, m_initiatorMailbox, m_zkMailboxNode, m_whoami);
    }
}
