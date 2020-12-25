# INF727-Systemes_repartis

DAGandCo is a pseudo DAG (Directed Acyclic Graph) builder application to visualize in real time the execution of a MapReduce system. Its development is part of the INF 727 course given at Télecom Paris dealing with distributed systems.
This application, developped in Python, is available with two versions of MapReduce system used to make a wordcount job:
- the version v3.0 is the non-optimized version 
- the version v3.1 is the optimized one

## Environment

First of all, it is important to know that the MapReduce system uses the ***split*** unix command here. It is therefore necessary to have a unix system (Linux or Mac).   
What's more, DAGandCo requires the installation of ***dash*** and ***dash_cytoscape*** packages from [PyPI](https://pypi.org/project/shap)

<pre>
pip install dash
pip install dash_cytoscape
</pre>

Other packages are used but are directly empacked if you have downloaded Python via Anaconda.   
Finally, the list of packages used in the application and whose versions may cause problems are as follows:

<pre>
dash.__version__ == 1.17.0
dash_cytoscape.__version__ == 0.2.0
pandas.__version__ == 1.1.4
</pre>

## Getting started

DAGandCo needs 3 inputs to launch a wordcount MapReduce process:
1. the list of computers that make up the cluster;
2. the username used for identification at each computer of the cluster;
3. the path of the file on which to apply the wordcount.

<p align="center">
  <img src="https://github.com/MathiasNourry/INF727-Systemes_repartis/blob/main/Explanation_1.png" width="1000" />
</p>


Once the entries have been filled in from the DAGandCo interface, you can launch the job.   
You should see the DAG being built in real time as the MapReduce process is running.   

![](https://github.com/MathiasNourry/INF727-Systemes_repartis/blob/main/Explanation_2.gif)


## Notation 
A colour code is used in DAGandCo:
- Green node = the computer has performed its task correctly;
- Blue node = occurs during a SHUFFLE if a computer didn't receive files to REDUCE;
- Orange node = this colour may appear if, during SHUFFLE, at least one SSH command to transfer a file between two computers in the cluster has failed. It thus reflects a loss of information;
- Red node = reports a fatal error on the computer, the task could not be performed.
