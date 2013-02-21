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
    MetaData = []
    TxMap = {}                              # Map each tranasction to an integer    

    DependencyGraph = nx.DiGraph()          # The actual dependency graph

    NumIOs = 0                              # Total #IOs used during materialization
    NumMaterialized = 0                     # The #TXs materialized

    ReadIOs = 0                             # Total IOs used for materializing reads
    NumReads = 0                            # # Materializing reads
    FreeIOs = 0

    Record = -1
    SortedTxList = []

    def insert_sorted_tx_list(tx):
        num_ios = G.TxMap[tx]['Cost']
        index = 0

        for cur_tx in G.SortedTxList:
            cur_cost = G.TxMap[cur_tx]['Cost']
            if num_ios > cur_cost:
                break
            else:
                index += 1
        
        G.SortedTxList.insert(index, tx)
        
    insert_sorted_tx_list = staticmethod(insert_sorted_tx_list)

    def write_dag_size(time, size):
        G.dag_size_file.write(str(time) + ':' + str(size) + '\n')

    write_dag_size = staticmethod(write_dag_size)

class BgRead(SimPy.Simulation.Process):
    def __init__(self):
        SimPy.Simulation.Process.__init__(self)
        
    def FindRead():
        maxRecord = -1
        maxIO = -1
            
        for record in G.LastWrite:
            if G.LastWrite[record]['IO'] > maxIO:
                maxRecord = record
                maxIO = G.LastWrite[record]['IO']
        
        assert maxIO != -1 and maxRecord != -1
        return maxRecord

    FindRead = staticmethod(FindRead)
    
    def Run(self):
        while 1:
            if G.LastWrite:
                GenRead.Read(BgRead.FindRead(), True)
            yield SimPy.Simulation.hold, self, G.Rnd.expovariate(BgRead.BgMatRate)
        

class GenRead(SimPy.Simulation.Process):

    
    def __init__(self):
        SimPy.Simulation.Process.__init__(self)

        
    
    def BackwardsBFS(first):
        queue = [first]
        done = []
        
        while queue:
            if not(queue[0] in done):
                queue = queue + G.DependencyGraph.predecessors(queue[0])
                done.append(queue[0])
                
            del queue[0]
        return done
            
        
    BackwardsBFS = staticmethod(BackwardsBFS)



    def Read(n, isFree):        
        tot = 0
        numMat = 0
        if n in G.LastWrite:
            
            # Since we've already stored the records that will
            # be materialized as a result of this transaction in the
            # LastWrite dictionary, we don't have to do anything more
            # to figure out the number of IOs for this materialization.
            #
            # However, we still have to update the graph to reflect the
            # dependency structure properly.
            tx = G.LastWrite[n]['Tx']

            # From this point onwards, everything is oriented towards
            # updating information in the dependency graph.
            txList = GenRead.BackwardsBFS(tx)
            
            # It is safe to execute transactions in sorted order because
            # there are no unknown dependencies.
            txList.sort()

            done = set([])
            
            inMem = []

            # Iterates through the list of transactions that are executed
            # as a result of this materialization.            
            for tx in txList:
                
                # The transaction has already been processed. This can happen
                # because we don't remove duplicates from the list and two
                # different transactions could be dependent on the same transaction.
                if tx in done:
                    continue

                # Print this for debugging purposes. The only case in which this
                # stuff will print out is if the assertion that follows 
                # is false.
                #
                # The invariant that we're maintaining is that the transactions are
                # being processed in topologically sorted order. This means that in
                # order to be able to execute a transaction, it must not be dependent
                # on any other transaction, ie, it must be a root.
                if not(tx in G.Roots):

                    print G.DependencyGraph.predecessors(tx)
                    print G.DependencyGraph.successors(tx)
                    print txList
                    print tx

                assert tx in G.Roots

                children = G.DependencyGraph.successors(tx)
                G.DependencyGraph.remove_node(tx)  
                
                done.add(tx)
                G.Roots.remove(tx)                    
                assert not (tx in G.Roots)               

                for child in children:
                    if not G.DependencyGraph.predecessors(child):
                        G.Roots.add(child)
                
                records = G.TxMap[tx]['Tx']
                
                for record in records:
                    if G.LastWrite[record]['Tx'] == tx:
                        del G.LastWrite[record]
                    else:
                        G.LastWrite[record]['IO'] -= 1;
                        #GenTx.UpdateMeta(record)
                    
                del G.TxMap[tx]

                G.SortedTxList.remove(tx)
                inMem += records
                
                
            if not isFree:
                tot = 2 * len(set(inMem))
            else:
                tot = 0
                G.FreeIOs += 2 * len(set(inMem))

                    
            numMat = len(done)

                
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
            GenRead.Read(record, False)
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
                # Keep some meta-data along with the record number.
                # We're going to use this later on to find the number
                # of I/Os required to materialize this Tx.
                retTx.append(hotItem)

            # All the other records are cold records. 
            # The first cold record is one greater than the
            # last hot record. 
            else:
                coldItem = random.choice(G.ColdList)
                while coldItem in retTx:
                    coldItem = random.choice(G.ColdList)
                    
                # Meta data update again. This is the same
                # as that for hot records. We're going to 
                # update this while finding dependencies.
                retTx.append(coldItem)

        ret['Tx'] = retTx
        G.TxMap[ret['TxNo']] = ret
        return ret

    GenTransaction = staticmethod(GenTransaction)
    
    def InsertTx(t):
        isRoot = True 
        index = t['TxNo']
        
        G.DependencyGraph.add_node(index)

        recordSet = list(t['Tx'])

        cnt = 0
        for record in t['Tx']:
            cnt += 1
            # Check if there is a tx that writes to this record.
            #
            # If yes, we have to add a dependency between this tx
            # and the last writer.

            if record in G.LastWrite:
                isRoot = False                
                assert (G.LastWrite[record]['Tx'] != index)       
                assert G.LastWrite[record]['Tx'] in G.DependencyGraph.nodes()

                G.DependencyGraph.add_edge(G.LastWrite[record]['Tx'], index)
                G.LastWrite[record]['Tx'] = index
                G.LastWrite[record]['IO'] += 1
            else:
                G.LastWrite[record] = {}
                G.LastWrite[record]['Tx'] = index
                G.LastWrite[record]['IO'] = 1
            
            #GenTx.UpdateMeta(record)

        if isRoot:
            G.Roots.add(t['TxNo'])        
        
        GenTx.TxCost(t['TxNo'])
        G.insert_sorted_tx_list(t['TxNo'])
                
    InsertTx = staticmethod(InsertTx)

    def TxCost(t):
        GenRead.BackwardsBFS(t)
        allRecords = []
        for tx in GenRead.BackwardsBFS(t):
            allRecords += G.TxMap[tx]['Tx']
        
        G.TxMap[t]['Cost'] = len(set(allRecords))
        print G.TxMap[t]['Cost']
    
    TxCost = staticmethod(TxCost)

    def UpdateMeta(record):
        # Try to remove the record from the sorted list, if we fail, it's ok.
        try:
            G.MetaData.remove(record)
        except:
            pass


        if record in G.LastWrite:
        
            # Find the new count of the record.
            count = G.LastWrite[record]['IO']
            
            # If the list is empty then we don't have to do anything
            if not G.MetaData:
                G.MetaData.append(record)
            
            else:
                length = len(G.MetaData)
                i = 0

                # Search for the right position and insert it.            
                for r in G.MetaData:
                    curCount = G.LastWrite[r]['IO']
                    if curCount < count:
                        break
                    i += 1
                    G.MetaData.insert(i, record)

        print G.MetaData
            
    UpdateMeta = staticmethod(UpdateMeta)
    
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
    G.dag_size_file = open('dag_size.txt', 'a')

    # Initialize the static members of the materialization class.
    # Materialize.Process = Materialize.FIFORoot    
    
    GenRead.ReadRate = 1 / float(sys.argv[1])
    BgRead.BgMatRate = 1 / float(sys.argv[2])
    SimPy.Simulation.initialize()
    
    rtx = GenRead()
    tx = GenTx()
    bg = BgRead()

    SimPy.Simulation.activate(tx, tx.Run())
    SimPy.Simulation.activate(rtx, rtx.Run())
    SimPy.Simulation.activate(bg, bg.Run())
    MaxSimTime = 100.0
    SimPy.Simulation.simulate(until = MaxSimTime)

    G.dag_size_file.close()
    
    rResults = open('reads.txt', 'a')
    readResult = float(G.ReadIOs) / float(G.NumReads)
    
    rResults.write(str(GenTx.TxRate / GenRead.ReadRate) + ":" + str(readResult)+'\n')
    rResults.close()

    mResults = open('materialized.txt', 'a')
    matResult = float(G.ReadIOs) / float(G.NumMaterialized)

    fResults = open('free.txt', 'a')
    freeResults = float(G.FreeIOs) / float(G.NumReads)
    fResults.write(str(freeResults) + '\n')
    
    mResults.write(str(GenTx.TxRate / GenRead.ReadRate) + ":" + str(matResult)+'\n')
    mResults.close()
    
    print 'done!!!'
        
if __name__ == '__main__':
    main()
