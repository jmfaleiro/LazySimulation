import SimPy.Simulation
import random
import networkx as nx

# TODO: 1. Vary the kinds of transactions
#       
# I'm assuming that there are 100 tuples and that
# a certain fraction of them are hot, both
# written and read very often. 

class G:
    Rnd = random.Random(12345)
    
    # Dependency graph specific stuff

    Roots = []               # The first frontier of txs to materialize
    Writers = {}             # The last writer of a tuple

    ReadIOs = []

    PendingTransactions = []
    DoneTransactions = []

    DependencyGraph = nx.DiGraph()


# This class is used to generate transactions in the
# same manner as described in the microbenchmark in the
# Calvin paper. 
#
# Generate a read set of NumRecords transactions and update all
# of them. One of the reads is to a 'hot' record.
#
# We can control the rate at which these transactions enter
# the system. We should also control the number of hot records,
# this will directly affect the contention index (from Calvin).

class GenTx(SimPy.Simulation.Process):
    TxRate = 1/0.01
    TP = 0
    HP = 0
    CP = 0

    def __init__(self, db):
        SimPy.Simulation.Process.__init__(self)
        self.database = db

    def GenTransaction():
        # First set the globally unique TxNo. This reads from
        # an increasing counter (TP).
        ret = { 'TxNo': GenTx.TP }
        GenTx.TP++        
        retTx = []
        
        # Generate the read set. This is done in the same 
        # manner as the Calvin paper.
        for i in range(0, GenTx.NumRecords):
            
            # The first record is always a hot record. 
            # We have a pointer to hot record Identifiers (HP).
            # HP is incremented modulo the number of hot records
            # to generate the hot record of the subsequent tx.
            if i == 0:
                retTx.append(GenTx.HP)
                GenTx.HP = (GenTx.HP+1) % GenTx.NumHot

            # All the other records are cold records. 
            # The first cold record is one greater than the
            # last hot record. 
            else:
                retTx.append(GenTx.CP)
                GenTx.CP = ((GenTx.CP+1) % GenTx.NumCold) + GenTx.NumHot

        ret['Tx'] = retTx
        G.TxMap[ret['TxNo']] = retTx

    GenTransaction = staticmethod(GenTransaction)
    
    def InsertTx(t):
        isRoot = True 
        G.DependencyGraph.add_node(t['TxNo'])
        
        for record in t['Tx']:
            
            # Check if there is a tx that writes to this record.
            #
            # If yes, we have to add a dependency between this tx
            # and the last writer.
            if record in G.LastWrite:
                isRoot = False
                G.DependencyGraph.add_edge(t['TxNo'], G.LastWrite[record])
            
            # This transaction is now the last writer to this
            # record.
            G.LastWrite[record] = t['TxNo']
            
        if isRoot:
            G.Roots.append(t)
    
    InsertTx = staticmethod(InsertTx)
    
    def Run(self):
        while 1:
            curTx = GenTx.GenTransaction()
            GenTx.InsertTx(curTx)
            yield SimPy.Simulation.hold, self, G.Rnd.expovariate(IncomingGraphTransactions.TxRate)
                



# This class is used to simulate transactions
# coming into the system.
class IncomingTransactions(SimPy.Simulation.Process):
    TxRate = 1/0.1 # 1.0 is the mean time between transactions
    
    def __init__(self, db):
        SimPy.Simulation.Process.__init__(self)
        self.database = db
    
    def Run(self):
        while 1:
            tx = SimPy.Simulation.now()
            G.PendingTransactions.append(tx)
            nextTxTime = G.Rnd.expovariate(IncomingTransactions.TxRate)
            if len(G.PendingTransactions) == 1:
                SimPy.Simulation.reactivate(self.database)
            yield SimPy.Simulation.hold, self, nextTxTime


# This class is used to simulate processing transactions
# within the system.
#
# We probably have to make the model more complicated than
# what we have at the moment. For example, we're basically saying
# that transactions are executing one by one here, there is no 
# concurrency. How will you simulate this?

class TraditionalDB(SimPy.Simulation.Process):
    NumTrans = 0
    NumWaiting = 0
    TimeWaiting = 0.0
    ProcessingRate = 1/1.0 # 1.0 is the mean time to process a transaction
    
    def __init__(self):
        SimPy.Simulation.Process.__init__(self)
        
    def Run(self):
        while 1:
            if not G.PendingTransactions:
                yield SimPy.Simulation.passivate, self
            
            doneTime = G.Rnd.expovariate(TraditionalDB.ProcessingRate)
            yield SimPy.Simulation.hold, self, doneTime
            G.DoneTransactions.append({'start' : G.PendingTransactions[0], 'end' : G.PendingTransactions[0]+doneTime})
            del(G.PendingTransactions[0])

# Assume that each transaction performs
# 2 I/Os, one for read, one for write.
# 
# The structure of the graph will vary with
# how you select the reads and writes. 

class DumbLazyDB(SimPy.Simulation.Process):
    DependencyGraph = nx.DiGraph()
    ProcessingRate = 1/0.1
    TxTable = {}
    TxId = 0
    NumIO = 0
    NumTx = 0
    Threshold = 20
    
    def __init__(self):
        SimPy.Simulation.Process.__init__(self)

    def Run(self):
        while 1:
            if len(DumbLazyDB.DependencyGraph.nodes()) < DumbLazyDB.Threshold:
                yield SimPy.Simulation.passivate, self
            
            x = checkPopularRoot()
            # We just found a popular root, materialize it.
            if x:
                materializeSingle(DumbLazyDB.DependencyGraph, x)
            else:
                materializeSingle(DumbLazyDB.DependencyGraph, G.Roots[0])
                
            DumbLazyDB.NumIO += 2
            DumbLazyDB.NumTx += 1
            yield SimPy.Simulation.hold, self, G.Rnd.expovariate(DumbLazyDB.ProcessingRate)
                
def materializeSingle(graph, root):
    #assert not graph.predecessors(root)
    # First add to the list of roots.

    G.Roots.extend(graph.successors(root))
    # Update graph statistics
    wRecord = DumbLazyDB.TxTable[root]['write']
    rRecord = DumbLazyDB.TxTable[root]['read']
    
    G.Writers[wRecord].remove(root)
    
    #graph.remove_node(root)
    G.Roots.remove(root)

            
def checkPopularRoot():
    for record in G.Roots:
        if DumbLazyDB.TxTable[record]['write'] < 10:
            return record
    return None
            

def main():
    SimPy.Simulation.initialize()
    db = DumbLazyDB()
    tx = IncomingGraphTransactions(db)
    
    SimPy.Simulation.activate(db, db.Run())
    SimPy.Simulation.activate(tx, tx.Run())

    MaxSimTime = 1000.0
    SimPy.Simulation.simulate(until = MaxSimTime)

    print DumbLazyDB.NumIO
    
            
                
        
if __name__ == '__main__':
    main()
