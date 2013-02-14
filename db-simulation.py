import SimPy.Simulation
import random
import networkx as nx
import types
import sys

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

    
    def __init__(self):
        SimPy.Simulation.Process.__init__(self)

        
    
    def BackwardsBFS(first):
        queue = [first]
        done = []
        
        while queue:
            if queue[0] in done:
                done.append(queue[0])
                del queue[0]
            else:
                queue = queue + G.DependencyGraph.predecessors(queue[0])
                done.append(queue[0])
                del queue[0]
        return done
            
        
    BackwardsBFS = staticmethod(BackwardsBFS)

    def Read(n):        
        tot = 0
        numMat = 0
        if n in G.LastWrite:

            txList = GenRead.BackwardsBFS(G.LastWrite[n])
            txList.sort()
            #print txList
            done = {}
            inMem = []
            while txList:
                if txList[0] in done:
                    del txList[0]
                    continue
                
                #print 1
                records = G.TxMap[txList[0]]
                inMem += records
                tx = txList[0]

                
                if not(tx in G.Roots):
                    #print done
                    for t in done:
                        print G.DependencyGraph.predecessors(t)
                        print G.DependencyGraph.successors(t)
                        print 'blah'
                        
                    print G.DependencyGraph.predecessors(tx)
                    print G.DependencyGraph.successors(tx)
                    print txList
                    print tx

                assert tx in G.Roots
                done[tx] = -1
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
                    
            numMat = len(set(done.keys()))
            tot = 2*len(set(inMem))
                
        else:
            tot = 1
            
        #print numMat
        G.NumMaterialized += numMat
        G.ReadIOs += tot
        G.NumReads += 1
        
        #print G.ReadIOs
        #print G.NumReads
        #print G.NumMaterialized
        
    Read = staticmethod (Read)
    
    def GenerateRead():
        temp = random.choice(G.HotList)
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
            if i < 8:
                hotItem = random.choice(G.HotList)
                while hotItem in retTx:
                    hotItem = random.choice(G.HotList)
                retTx.append(hotItem)

            # All the other records are cold records. 
            # The first cold record is one greater than the
            # last hot record. 
            else:
                coldItem = random.choice(G.ColdList)
                while coldItem in retTx:
                    coldItem = random.choice(G.ColdList)

                retTx.append(coldItem)

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
            yield SimPy.Simulation.hold, self, G.Rnd.expovariate(GenTx.TxRate)


    

def main():
    # Initialize the static members of the transaction generation class.
    numHot = 100
    G.HotList = range(0, numHot)
    G.ColdList = range(numHot, 10000000)
    G.NumRecords = 10    

    # Initialize the static members of the materialization class.
    # Materialize.Process = Materialize.FIFORoot    
    
    GenRead.ReadRate = 1 / float(sys.argv[1])
    SimPy.Simulation.initialize()
    
    rtx = GenRead()
    tx = GenTx()

    SimPy.Simulation.activate(tx, tx.Run())
    SimPy.Simulation.activate(rtx, rtx.Run())
    MaxSimTime = 1000.0
    SimPy.Simulation.simulate(until = MaxSimTime)
    
    rResults = open('reads.txt', 'a')
    readResult = float(G.ReadIOs) / float(G.NumReads)
    
    rResults.write(str(GenTx.TxRate / GenRead.ReadRate) + ":" + str(readResult)+'\n')
    rResults.close()

    mResults = open('materialized.txt', 'a')
    matResult = float(G.ReadIOs) / float(G.NumMaterialized)
    
    mResults.write(str(GenTx.TxRate / GenRead.ReadRate) + ":" + str(matResult)+'\n')
    mResults.close()
    
    print 'done!!!'
        
if __name__ == '__main__':
    main()
