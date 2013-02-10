import SimPy.Simulation
import random
import networkx as nx
import types

# TODO: 1. Vary the kinds of transactions
#       
# I'm assuming that there are 100 tuples and that
# a certain fraction of them are hot, both
# written and read very often. 

class G:
    Rnd = random.Random(12345)
    
    # Dependency graph specific stuff

    Roots = []                              # The first frontier of txs to materialize
    LastWrite = {}                          # The last writer of a tuple
    TxMap = {}                              # Map each tranasction to an integer    
    NumIOs = 0                              # Total #IOs used during materialization
    DependencyGraph = nx.DiGraph()          # The actual dependency graph


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
        self.DB = db

    def GenTransaction():
        # First set the globally unique TxNo. This reads from
        # an increasing counter (TP).
        ret = {  }
        ret['TxNo'] = GenTx.TP

        GenTx.TP += 1        
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
        return ret

    GenTransaction = staticmethod(GenTransaction)
    
    def InsertTx(t):
        isRoot = True 
        index = t['TxNo']
        G.DependencyGraph.add_node(index)
        
        for record in t['Tx']:
            
            # Check if there is a tx that writes to this record.
            #
            # If yes, we have to add a dependency between this tx
            # and the last writer.
            if record in G.LastWrite:
                isRoot = False
                G.DependencyGraph.add_edge(G.LastWrite[record], index)
            
            # This transaction is now the last writer to this
            # record.
            G.LastWrite[record] = index
            
        if isRoot:
            assert isinstance(t['TxNo'], int)
            G.Roots.append(t['TxNo'])
    
    InsertTx = staticmethod(InsertTx)
    
    def Run(self):
        while 1:
            curTx = GenTx.GenTransaction()
            GenTx.InsertTx(curTx)
            SimPy.Simulation.reactivate(self.DB)
            yield SimPy.Simulation.hold, self, G.Rnd.expovariate(GenTx.TxRate)
                
class Materialize(SimPy.Simulation.Process):

    def __init(self):
        SimPy.Simulation.Process.__init__(self)

    # This method processes all roots in FIFO order.    
    def FIFORoot():
        if not G.Roots:
            return False
        else:
            t = G.Roots[0]
            del G.Roots[0]
            G.NumIOs += 2*(GenTx.NumRecords)
            G.Roots = G.Roots+G.DependencyGraph.successors(t)
            G.DependencyGraph.remove_node(t)
            
            # Update all last write information: In case this was
            # the last write of a record, update the info.
            for record in G.TxMap[t]:
                if G.LastWrite[record] == t:
                    G.LastWrite.pop(record)

            return True

    FIFORoot = staticmethod(FIFORoot)

    def Run(self):
        while 1:
            if not Materialize.FIFORoot():
                yield SimPy.Simulation.passivate, self
            

def main():
    # Initialize the static members of the transaction generation class.
    GenTx.NumHot = 1000
    GenTx.NumCold = 1000000
    GenTx.NumRecords = 10

    # Initialize the static members of the materialization class.
    # Materialize.Process = Materialize.FIFORoot
    
    SimPy.Simulation.initialize()    
    
    db = Materialize()
    tx = GenTx(db)
    
    SimPy.Simulation.activate(db, db.Run())
    SimPy.Simulation.activate(tx, tx.Run())

    MaxSimTime = 1000.0
    SimPy.Simulation.simulate(until = MaxSimTime)

    print G.NumIOs            
                
        
if __name__ == '__main__':
    main()
