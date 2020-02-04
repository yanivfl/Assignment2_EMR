# Assignment2_dsps
# Distributed System - Assignment 2

#### Yaniv Fleischer (yanifl) 203817002
#### Yuval Lahav (lahavyuv) 205689110

<br/><br/>
## Running Instructions:
1. Create a pair key (AWS) with the name: "YuvalKeyPair.pem" 
2. Open a 2 bucket in your S3, one for input and another for the output
3. Change in the Assignment on the Constant file the following variables:
 * INPUT_BUCKET_NAME
 * OUTPUT_BUCKET_NAME
 * MY_JAR_NAME
4. Create a jar from the assignment and upload it to the input bucket in S3 you created
5. Run the java command from the assignment:
"java -jar Assignment2.jar"


<br/><br/>
## Link to output file
TODO
_note_: If you followed the instructions, the file should appear in your output bucket under the name: final_output
 

<br/><br/>
## Statistics 
path: logs/<change>/hadoop-mapreduce/history/2020/02/<change>/000000
JOB_1_GRAM:
<br/>    	Map input records=
<br/>		Map output records=
<br/>		Combine input records=
<br/>		Combine output records=
<br/>		Reduce input records=
<br/>		Reduce output records=

-------------------------------------------

JOB_2_GRAM:
<br/>		Map input records=
<br/>		Map output records=
<br/>		Combine input records=
<br/>		Combine output records=
<br/>		Reduce input records=
<br/>		Reduce output records=

-------------------------------------------

JOB_3_GRAM:
<br/>		Map input records=
<br/>		Map output records=
<br/>		Combine input records=
<br/>		Combine output records=
<br/>		Reduce input records=
<br/>		Reduce output records=

-------------------------------------------

 JOB_C0
<br/>		Map input records=
<br/>		Map output records=
<br/>		Combine input records=
<br/>		Combine output records=
<br/>		Reduce input records=
<br/>		Reduce output records=

-------------------------------------------

JOB_JOIN_N1
<br/>		Map input records=
<br/>		Map output records=
<br/>		Reduce input records=
<br/>		Reduce output records=

-------------------------------------------

JOB_JOIN_N2
<br/>		Map input records=
<br/>		Map output records=
<br/>		Reduce input records=
<br/>		Reduce output records=

-------------------------------------------

JOB_JOIN_C1
<br/>		Map input records=
<br/>		Map output records=
<br/>		Reduce input records=
<br/>		Reduce output records=

-------------------------------------------

JOB_JOIN_C2
<br/>		Map input records=
<br/>		Map output records=
<br/>		Reduce input records=
<br/>		Reduce output records=
    
    -------------------------------------------

JOB_JOIN_C0
<br/>		Map input records=
<br/>		Map output records=
<br/>		Reduce input records=
<br/>		Reduce output records=
    
    -------------------------------------------

JOB_PROB_WITH_SORT
<br/>		Map input records=
<br/>		Map output records=
<br/>		Reduce input records=
<br/>		Reduce output records=

    -------------------------------------------
    
    
_Master instance type:_ M1Large

_Slave instance type:_ M1Large

_Number of instances:_ 5 slaves (6 instance)
