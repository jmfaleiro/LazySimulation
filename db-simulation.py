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

    Roots = set([])                         # The first frontier of txs to materialize
    LastWrite = {}                          # The last writer of a tuple
    TxMap = {}                              # Map each tranasction to an integer    

    DependencyGraph = nx.DiGraph()          # The actual dependency graph

    NumIOs = 0                              # Total #IOs used during materialization
    NumMaterialized = 0                     # The #TXs materialized

    ReadIOs = 0                             # Total IOs used for materializing reads
    NumReads = 0                            # # Materializing reads

    Record = -1
    

class GenRead(SimPy.Simulation.Process):
    ReadRate = 1/0.01
    
    def __init__(self):
        SimPy.Simulation.Process.__init__(self)

        
    
    def BackwardsBFS(q, done):
        if not q:
            return done

        elif q[0] in done:
            done.append(q[0])
            del q[0]
            return GenRead.BackwardsBFS(q, done)
        
        else:
            if G.DependencyGraph.predecessors(q[0]):
                q = q + G.DependencyGraph.predecessors(q[0])
            else:
                assert(q[0] in G.Roots)

            done.append(q[0])
            del q[0]
            return GenRead.BackwardsBFS(q, done)
        
    BackwardsBFS = staticmethod(BackwardsBFS)

    def Read(n):        
        tot = 0
        numMat = 0
        if n in G.LastWrite:
            txList = []
            GenRead.BackwardsBFS([G.LastWrite[n]], txList)
            txList.sort()
            #print txList
            done = []
            inMem = []
            while txList:
                if txList[0] in done:
                    continue

                
                records = G.TxMap[txList[0]]
                diff = set(records) - set(inMem)
                tot += 2*len(diff)                
                inMem += records
                tx = txList[0]

                
                if not(tx in G.Roots):
                    print done
                    for t in done:
                        print G.DependencyGraph.predecessors(t)
                        print G.DependencyGraph.successors(t)
                        print 'blah'
                        
                    print G.DependencyGraph.predecessors(tx)
                    print G.DependencyGraph.successors(tx)
                    print txList
                    print tx

                assert tx in G.Roots
                done.append(txList[0])
                del txList[0]

                G.Roots.remove(tx)                    
                assert not (tx in G.Roots)                
                G.Roots.update(set(G.DependencyGraph.successors(tx)))

                G.DependencyGraph.remove_node(tx)                
                
                # Update all last write information: In case this was
                # the last write of a record, update the info.
                for record in records:
                    if G.LastWrite[record] == tx:
                        del G.LastWrite[record]
                    
            numMat = len(done)
                
        else:
            tot += 1

        G.NumMaterialized += numMat
        G.ReadIOs += tot
        G.NumReads += 1

    Read = staticmethod (Read)
    
    def GenerateRead():
        hot = range(0, G.NumHot)
        temp = random.choice(hot)
        return  temp
        
    GenerateRead = staticmethod (GenerateRead)

    def Run(self):
        while 1:
            record = GenRead.GenerateRead()
            GenRead.Read(record)
            yield SimPy.Simulation.hold, self, G.Rnd.expovariate(GenRead.ReadRate)


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
    TxRate = 1/0.001
    TP = 0
    HP = 0
    CP = 0
    
    def __init__(self):
        SimPy.Simulation.Process.__init__(self)
#        self.DB = db

    def GenTransaction():
        # First set the globally unique TxNo. This reads from
        # an increasing counter (TP).
        ret = {  }
        ret['TxNo'] = GenTx.TP

        GenTx.TP += 1        
        retTx = []
        
        # Generate the read set. This is done in the same 
        # manner as the Calvin paper.
        for i in range(0, G.NumRecords):
            
            # The first record is always a hot record. 
            # We have a pointer to hot record Identifiers (HP).
            # HP is incremented modulo the number of hot records
            # to generate the hot record of the subsequent tx.
            if i == 0:
                retTx.append(GenTx.HP)
                GenTx.HP = (GenTx.HP+1) % G.NumHot

            # All the other records are cold records. 
            # The first cold record is one greater than the
            # last hot record. 
            else:
                retTx.append(GenTx.CP)
                GenTx.CP = ((GenTx.CP+1) % G.NumCold)
                if GenTx.CP < G.NumHot:
                    GenTx.CP += G.NumHot

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
                
                if G.LastWrite[record] == index:
                    print index

                assert (G.LastWrite[record] != index)
                G.DependencyGraph.add_edge(G.LastWrite[record], index)
            
            # This transaction is now the last writer to this
            # record.
            G.LastWrite[record] = index
            
        if isRoot:
            assert isinstance(t['TxNo'], int)
            assert not(t['TxNo'] in G.Roots)
            G.Roots.add(t['TxNo'])
    
    InsertTx = staticmethod(InsertTx)
    
    def Run(self):
        while 1:
            curTx = GenTx.GenTransaction()
            GenTx.InsertTx(curTx)
#            SimPy.Simulation.reactivate(self.DB)
            yield SimPy.Simulation.hold, self, G.Rnd.expovariate(GenTx.TxRate)
            

def main():
    # Initialize the static members of the transaction generation class.
    G.NumHot = 1000
    G.NumCold = 1000000
    G.NumRecords = 10

    GenTx.CP = G.NumHot

    # Initialize the static members of the materialization class.
    # Materialize.Process = Materialize.FIFORoot
    
    SimPy.Simulation.initialize()    
    
    rtx = GenRead()
    tx = GenTx()
    
#    SimPy.Simulation.activate(db, db.Run())

    SimPy.Simulation.activate(tx, tx.Run())
    SimPy.Simulation.activate(rtx, rtx.Run())
    MaxSimTime = 1000.0
    SimPy.Simulation.simulate(until = MaxSimTime)

    print G.ReadIOs / G.NumReads
    print G.ReadIOs / G.NumMaterialized

                
        
if __name__ == '__main__':
    main()
