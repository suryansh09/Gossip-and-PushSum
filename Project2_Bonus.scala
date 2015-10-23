import akka.actor._
import akka.actor.Terminated
import akka.actor.actorRef2Scala
import akka.actor.Actor
import akka.dispatch.Foreach
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.LoggingReceive
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import java.io._
import java.security.MessageDigest
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set


object project2_Bonus{
	var actorCollection = new HashMap[String, ActorRef]()
	var topologyNoOfNodes = 0
	var percentageOfNodesDead:Double = 10.00
	var deadTimer = 10
	var startTime = System.currentTimeMillis
	var hs = new HashMap[Int, String]()
	var nrOfNodesToKill = 0
	var nodesKilled: Boolean = false
	var nrOfNodes = 0 
	object Topology{
	
		
		var imperfecths = new HashMap[Int, Set[Int]]()
		def add(nodeName: String, nodeNum: Int): Unit = {
			// Create the Topology
			hs += (nodeNum -> nodeName)			
		}
		def remove(nodeName: String) : Unit = {
			var temp = nodeName.split(':')
			var key = temp(2).toInt
			hs -= (key)

			if(temp(0) == "Imperfect3DGrid"){
				imperfecths -= key
				var imperfecthskset = imperfecths.keySet
				var tempArray = new Array[Int](imperfecthskset.size)
				imperfecthskset.copyToArray(tempArray)
				for(i <- 0 to tempArray.length-1){
					var tempSet = imperfecths.getOrElse(tempArray(i),null)
					if(tempSet != null){
						tempSet -= key
						imperfecths -= tempArray(i)
						imperfecths += (tempArray(i) -> tempSet)
					}
				}
			}
		}
		def randomNeighbour(nodeName: String): String = {
			var temp = nodeName.split(':')
			if(temp(0) == "FullNetwork")
				return(randomNeighbourFullNetwork(nodeName))
			if(temp(0) == "3DGrid")
				return(randomNeighbourGrid3D(nodeName))
			if(temp(0) == "Line")
				return(randomNeighbourLine(nodeName))
			if(temp(0) == "Imperfect3DGrid")
				return(randomNeighbourImpGrid3D(nodeName))
			return null
		}
		def randomNeighbourFullNetwork(nodeName: String): String = {
			//Find all Neighbours
			//Return a Random Neighbour
			var temp = nodeName.split(':')
			var key = temp(2).toInt
			var hkset = hs.keySet
			hkset -= (key)
			if(hkset.size == 0)
				return null
			val rnd = new Random
			val range = new Array[Int](hkset.size)
			hkset.copyToArray(range)	
			hs.getOrElse(range(rnd.nextInt(range length)), "null")
		}
		
	

	
		def randomNeighbourGrid3D(nodeName: String): String = {
			//Find all Neighbours
			//Return a Random Neighbour
			var temp = nodeName.split(':')
			var key = temp(2).toInt
			var x = key / ((topologyNoOfNodes)*(topologyNoOfNodes))
			var y = (key - (x*topologyNoOfNodes*topologyNoOfNodes))/topologyNoOfNodes
			var z = key % topologyNoOfNodes
			var keyArray = new Array[Int](3)
			keyArray(0) = z
			keyArray(1) = y
			keyArray(2) = x
			var hkset: Set[Int] = Set()
			for(j <- 0 to 2){
				var temp1 = keyArray(j)+1
				var temp2 = keyArray(j)-1
				var tempKey1 = 0
				var tempKey2 = 0
				if(j == 0){
					tempKey1 = temp1 + keyArray(1)*topologyNoOfNodes + keyArray(2)*(topologyNoOfNodes*topologyNoOfNodes)
					tempKey2 = temp2 + keyArray(1)*topologyNoOfNodes + keyArray(2)*(topologyNoOfNodes*topologyNoOfNodes)
				}
				if(j == 1){
					tempKey1 = keyArray(0) + temp1*topologyNoOfNodes + keyArray(2)*(topologyNoOfNodes*topologyNoOfNodes)
					tempKey2 = keyArray(0) + temp2*topologyNoOfNodes + keyArray(2)*(topologyNoOfNodes*topologyNoOfNodes)
				}
				if(j == 2){
					tempKey1 = keyArray(0) + keyArray(1)*topologyNoOfNodes + temp1*(topologyNoOfNodes*topologyNoOfNodes)
					tempKey2 = keyArray(0) + keyArray(1)*topologyNoOfNodes + temp2*(topologyNoOfNodes*topologyNoOfNodes)
				}
				if(hs.getOrElse(tempKey1, null) != null && temp1 >=0 && temp1 < topologyNoOfNodes)
					hkset += tempKey1
				if(hs.getOrElse(tempKey2, null) != null && temp2 >=0 && temp2 < topologyNoOfNodes)
					hkset += tempKey2
			}
			if(hkset.size == 0)
				return null
			val rnd = new Random
			val range = new Array[Int](hkset.size)
			hkset.copyToArray(range)	
			hs.getOrElse(range(rnd.nextInt(range length)), "null")
		}
	
	

	
		def randomNeighbourLine(nodeName: String): String = {
			//Find all Neighbours
			//Return a Random Neighbour
			var temp = nodeName.split(':')
			var key = temp(2).toInt
			var keyForward = key + 1
			var keyBackward = key - 1
			var workerArray = new Array[String](2)
			workerArray(0) = hs.getOrElse(key+1,null)
			workerArray(1) = hs.getOrElse(key-1,null)
			if(workerArray(0) != null){
				if(workerArray(1) != null){
					val rnd = new Random
					return workerArray(rnd.nextInt(workerArray length))
				}
				return workerArray(0)	
			}
			if(workerArray(1) != null)
				return workerArray(1)
			return null			
		}
	
		def randomNeighbourImpGrid3D(nodeName: String): String = {
			//Find all Neighbours
			//Return a Random Neighbour
			var temp = nodeName.split(':')
			var key = temp(2).toInt
			if(imperfecths.getOrElse(key,null) == null){
				var x = key / ((topologyNoOfNodes)*(topologyNoOfNodes))
				var y = (key - (x*topologyNoOfNodes*topologyNoOfNodes))/topologyNoOfNodes
				var z = key % topologyNoOfNodes
				var keyArray = new Array[Int](3)
				keyArray(0) = z
				keyArray(1) = y
				keyArray(2) = x
				var hkset: Set[Int] = Set()
				for(j <- 0 to 2){
					var temp1 = keyArray(j)+1
					var temp2 = keyArray(j)-1
					var tempKey1 = 0
					var tempKey2 = 0
					if(j == 0){
						tempKey1 = temp1 + keyArray(1)*topologyNoOfNodes + keyArray(2)*(topologyNoOfNodes*topologyNoOfNodes)
						tempKey2 = temp2 + keyArray(1)*topologyNoOfNodes + keyArray(2)*(topologyNoOfNodes*topologyNoOfNodes)
					}
					if(j == 1){
						tempKey1 = keyArray(0) + temp1*topologyNoOfNodes + keyArray(2)*(topologyNoOfNodes*topologyNoOfNodes)
						tempKey2 = keyArray(0) + temp2*topologyNoOfNodes + keyArray(2)*(topologyNoOfNodes*topologyNoOfNodes)
					}
					if(j == 2){
						tempKey1 = keyArray(0) + keyArray(1)*topologyNoOfNodes + temp1*(topologyNoOfNodes*topologyNoOfNodes)
						tempKey2 = keyArray(0) + keyArray(1)*topologyNoOfNodes + temp2*(topologyNoOfNodes*topologyNoOfNodes)
					}
					if(hs.getOrElse(tempKey1, null) != null && temp1 >=0 && temp1 < topologyNoOfNodes)
						hkset += tempKey1
					if(hs.getOrElse(tempKey2, null) != null && temp2 >=0 && temp2 < topologyNoOfNodes)
						hkset += tempKey2
				}
				if(hkset.size == 0)
					return null
				val rnd = new Random
				val range = new Array[Int](hkset.size)
				hkset.copyToArray(range)	
				var tempKey = range(rnd.nextInt(range length))
				var mainhkset = hs.keySet
				for(i <- 0 to range.length-1)
					mainhkset -= range(i)
				mainhkset -= key
				hkset -= tempKey
				var mainarray = new Array[Int](mainhkset.size)
				mainhkset.copyToArray(mainarray)
				var newRandom = mainarray(rnd.nextInt(mainarray length))
				hkset += newRandom
				imperfecths += (key -> hkset)
			}
			var hkset = imperfecths.getOrElse(key, null)
			if(hkset == null)
				return null
			if(hkset.size == 0)
				return null
			val rnd = new Random
			val range = new Array[Int](hkset.size)
			hkset.copyToArray(range)	
			hs.getOrElse(range(rnd.nextInt(range length)), "null")
			
		}
	

	}
	object Worker{
		case object StartGossip
		case object StartPushSum
		case object Gossip
		case class PushSum(s:Double, w:Double)
	}
	class Worker extends Actor{
		import Worker._
		var maxRoumor = 10
		var nrOfRoumors = 0
		var pushSumInitialize: Boolean = false
		var s: Double = 0.0
		var w: Double = 1.0
		var ratioCurrent: Double = 0.0
		var ratioOld: Double = 0.0
		var pushSumCounter = 0
		def receive = {
			case StartGossip => {
				//Find One Random Neighbour
				//Gossip
				var randomNeighbour = Topology.randomNeighbour(self.path.name)
				var startWorker = actorCollection.getOrElse(randomNeighbour,null)
				if(startWorker != null)
					startWorker ! Worker.Gossip
				//randomNeighbour ! Gossip
			}
			case StartPushSum => {
				//Find One Random Neighbour
				//PushSum
				println("Debug1")
				pushSumInitialize = true
				var workerName = self.path.name
				var temp = workerName.split(':')
				s = temp(2).toDouble
				var randomNeighbour = Topology.randomNeighbour(workerName)
				var startWorker = actorCollection.getOrElse(randomNeighbour,null)
				if(startWorker != null){
					println("Debug2")
					startWorker ! Worker.PushSum(s, w)
				}

				
				
				
			}
			case Gossip => {
				//propogate
				//Terminate
				//Self Close
				if((nodesKilled == false) && (System.currentTimeMillis - startTime >= deadTimer)){
					nrOfNodesToKill = ((nrOfNodes * percentageOfNodesDead)/100).toInt
					var hkset = hs.keySet
					var temp = self.path.name.split(':')
					hkset -= (temp(2).toInt)
					val rnd = new Random
					if(nrOfNodesToKill > 0){
						for(i <- 0 to nrOfNodesToKill-1){
							val range = new Array[Int](hkset.size)
							hkset.copyToArray(range)	
							var tempKey = range(rnd.nextInt(range length))
							hkset -= (tempKey)
							var tempName = hs.getOrElse(tempKey,null)
							hs -= (tempKey)
							actorCollection -= (tempName)
							println("Node Failed: %s".format(tempName))
						}
						nodesKilled = true
					}
				}
				println("Rumuor Received: %s".format(self.path.name))
				nrOfRoumors = nrOfRoumors +1
				var randomNeighbour = Topology.randomNeighbour(self.path.name)
				var startWorker = actorCollection.getOrElse(randomNeighbour,null)
				if(startWorker != null){
					if(nrOfRoumors == maxRoumor){
						Topology.remove(self.path.name)
						actorCollection -= (self.path.name)
						println("Stopping: %s".format(self.path.name))
						//Master.Terminate
						//var getMaster = actorCollection.getOrElse("Master",null)
						//getMaster ! Master.Terminated
						context.stop(self)
					}
					startWorker ! Worker.Gossip
						
				}
					else{
						//Terminate if all Neighbours are Dead
						Topology.remove(self.path.name)
						actorCollection -= (self.path.name)
						println("Stopping: %s".format(self.path.name))
						//Master.Terminate
						var getMaster = actorCollection.getOrElse("Master",null)
						getMaster ! Master.Terminate
						context.stop(self)
					}
				//}
				//randomNeighbour ! Gossip
			}
			case PushSum(sNew:Double, wNew:Double) => {
				//propogate
				//Terminate
				//SelfClose
				if((nodesKilled == false) && (System.currentTimeMillis - startTime >= deadTimer)){
					nrOfNodesToKill = ((nrOfNodes * percentageOfNodesDead)/100).toInt
					var hkset = hs.keySet
					var temp = self.path.name.split(':')
					hkset -= (temp(2).toInt)
					val rnd = new Random
					if(nrOfNodesToKill > 0){
						for(i <- 0 to nrOfNodesToKill-1){
							val range = new Array[Int](hkset.size)
							hkset.copyToArray(range)	
							var tempKey = range(rnd.nextInt(range length))
							hkset -= (tempKey)
							var tempName = hs.getOrElse(tempKey,null)
							hs -= (tempKey)
							actorCollection -= (tempName)
							println("Node Failed: %s".format(tempName))
						}
						nodesKilled = true
					}
				}

				println("Rumuor Received: %s".format(self.path.name))
				if(pushSumInitialize == false){
					var temp = self.path.name.split(':')
					s = temp(2).toDouble
					pushSumInitialize = true
				}
				ratioOld = s/w
				s += sNew
				w += wNew
				ratioCurrent = s/w
				if(math.abs(ratioCurrent-ratioOld) < math.pow(10, -10))
					pushSumCounter += 1
				else
					pushSumCounter = 0
				var randomNeighbour = Topology.randomNeighbour(self.path.name)
				var startWorker = actorCollection.getOrElse(randomNeighbour,null)
				if(startWorker != null){
					if(pushSumCounter == 3){
						Topology.remove(self.path.name)
						actorCollection -= (self.path.name)
						println("Stopping: %s".format(self.path.name))	
						context.stop(self)
					}	
					s = s/2
					w = w/2
					startWorker ! Worker.PushSum(s, w)
					
				}
				else{
					Topology.remove(self.path.name)
					actorCollection -= (self.path.name)
					println("Stopping: %s".format(self.path.name))
					//Master.Terminate
					var getMaster = actorCollection.getOrElse("Master",null)
					getMaster ! Master.Terminate
					context.stop(self)
				}

			}
		}
	} 
	object Master{
		case object Start
		case object Terminate
	}
	class Master(nrOfNodes: Int, algorithm: String, topologyChoice: String) extends Actor{
		import Master._
		var closeCounter = 0;
		def receive = {
			case Start => {
				//instantiate all actors
				println("Algorithm Selected: %s".format(algorithm))
				println("Topology Selected: %s".format(topologyChoice))
								
				for(i <- 0 to nrOfNodes-1){
					var workerName = "%s:worker:%s".format(topologyChoice,i)
					val worker = context.actorOf(Props(new Worker), name = workerName)
					actorCollection += (workerName -> worker)
					println("Worker Created: %s".format(workerName))
					Topology.add(workerName, i)
				}
				startTime = System.currentTimeMillis
				if(algorithm == "Gossip"){
					//find a Random Actor 
					//StartGossip
					var akset = actorCollection.keySet
					akset -=("Master")
					val rnd = new Random
					val range = new Array[String](akset.size)
					akset.copyToArray(range)	
					var startWorker = actorCollection.getOrElse(range(rnd.nextInt(range length)), null)
					println("Random Worker Started: %s".format(startWorker.path.name))
					startWorker ! Worker.StartGossip
				}
				if(algorithm == "PushSum"){
					//find a Random Actor 
					//StartPushSum
					var akset = actorCollection.keySet
					akset -=("Master")
					val rnd = new Random
					val range = new Array[String](akset.size)
					akset.copyToArray(range)	
					var startWorker = actorCollection.getOrElse(range(rnd.nextInt(range length)), null)
					println("Random Worker Started: %s".format(startWorker.path.name))
					startWorker ! Worker.StartPushSum
				}
			}
			case Terminate => {
				//Print Time and ShutDown System
				
				if(actorCollection.size == 1){
					var duration = (System.currentTimeMillis - startTime)
					if(nodesKilled == true){
					println("Failure Occured in the Network After %s ms".format(deadTimer))
					println("Percentage of Nodes failed: %s".format(percentageOfNodesDead))
					println("Number of Nodes Failed: %s".format(nrOfNodesToKill))
				}
				else{
					println("No Failure in the Network")
				}
					println("System.ShutDown")
					println("Total Run Time: %s ms".format(duration))
					context.system.shutdown()
				}
				else{
					println("Network Restarting Because Of Node Deadlock...")
					var akset = actorCollection.keySet
					akset -=("Master")
					val rnd = new Random
					val range = new Array[String](akset.size)
					akset.copyToArray(range)	
					var worker = actorCollection.getOrElse(range(rnd.nextInt(range length)), null)
					worker ! Worker.Gossip
				}
			}
		}
	} 

	def main (args: Array[String]){
	    var topologyChoice = ""
	    var algorithm = ""
	    // exit if argument not passed as command line param
	    if (args.length < 3 || args.length > 4) {
	      println("Invalid no of args")
	      System.exit(1)
	    } 
	    else {
	    	nrOfNodes = args(0).toInt
	    	topologyChoice = args(1)
	    	algorithm = args(2)
	    	if(topologyChoice == "3DGrid" || topologyChoice == "Imperfect3DGrid"){
	    		var cube: Double = nrOfNodes
	    		nrOfNodes = math.round(math.cbrt(cube)).toInt
	    		topologyNoOfNodes = nrOfNodes
	    		nrOfNodes = nrOfNodes*nrOfNodes*nrOfNodes
	    	}
	    	if(args.length == 4){
	    		percentageOfNodesDead = args(3).toDouble
	    	}
	    	val system = ActorSystem.create("Propogation")
	    	val master = system.actorOf(Props(new Master(nrOfNodes, algorithm, topologyChoice)), name = "Master")
	    	actorCollection += ("Master" -> master)
	    	master ! Master.Start
	    }
	}

}