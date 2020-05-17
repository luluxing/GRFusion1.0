/*
 * PathScanExecutor.h
 *
 *  Created on: Nov 30, 2016
 *      Author: msaberab
 */

#ifndef SRC_EE_EXECUTORS_PATHSCANEXECUTOR_H_
#define SRC_EE_EXECUTORS_PATHSCANEXECUTOR_H_

#include <vector>
#include "boost/shared_array.hpp"
#include "common/common.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "executors/abstractexecutor.h"

namespace voltdb {

class AbstractExpression;
class TempTable;
class Table;
class TempTableLimits;
class AggregateExecutorBase;
class GraphView;
class CountingPostfilter; // LX

class PathScanExecutor : public AbstractExecutor {
public:
	PathScanExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node) : AbstractExecutor(engine, abstract_node) {
            //output_table = NULL;
            LogManager::GLog("PathScanExecutor", "Constructor", 31, abstract_node->debug());
        }
        ~PathScanExecutor();
    // modified by LX
    // using AbstractExecutor::p_init;
    // virtual protected bool p_init(AbstractPlanNode*, TempTableLimits* limits);

    protected:
        bool p_init(AbstractPlanNode*, const ExecutorVector& executorVector);// LX
        bool p_execute(const NValueArray &params);
        //void setTempOutputTable(TempTableLimits* limits, const string tempTableName);
    private:
        void outputTuple(CountingPostfilter& postfilter, TableTuple& tuple);
        AggregateExecutorBase* m_aggExec;
        GraphView* graphView;
};

}

#endif /* SRC_EE_EXECUTORS_PATHSCANEXECUTOR_H_ */
