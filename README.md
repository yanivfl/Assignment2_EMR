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
<br/>    	Map input records=44400490
<br/>		Map output records=44400490
<br/>		Combine input records=44400490
<br/>		Combine output records=645276
<br/>		Reduce input records=645276
<br/>		Reduce output records=645262

-------------------------------------------

JOB_2_GRAM:
<br/>		Map input records=252069581
<br/>		Map output records=252069581
<br/>		Combine input records=252069581
<br/>		Combine output records=4928634
<br/>		Reduce input records=4928634
<br/>		Reduce output records=4928490

-------------------------------------------

JOB_3_GRAM:
<br/>		Map input records=163471963
<br/>		Map output records=163471963
<br/>		Combine input records=163471963
<br/>		Combine output records=3295166
<br/>		Reduce input records=3295166
<br/>		Reduce output records=3172966

-------------------------------------------

 JOB_C0
<br/>		Map input records=645262
<br/>		Map output records=645262
<br/>		Combine input records=645262
<br/>		Combine output records=14
<br/>		Reduce input records=14
<br/>		Reduce output records=1

-------------------------------------------

JOB_JOIN_N1
<br/>		Map input records=3818228
<br/>		Map output records=3818228
<br/>		Reduce input records=3818228
<br/>		Reduce output records=2803960

-------------------------------------------

JOB_JOIN_N2
<br/>		Map input records=7732450
<br/>		Map output records=7732450
<br/>		Reduce input records=7732450
<br/>		Reduce output records=2803959

-------------------------------------------

JOB_JOIN_C1
<br/>		Map input records=3449221
<br/>		Map output records=3449221
<br/>		Reduce input records=3449221
<br/>		Reduce output records=2803959

-------------------------------------------

JOB_JOIN_C2
<br/>		Map input records=7732449
<br/>		Map output records=7732449
<br/>		Reduce input records=7732449
<br/>		Reduce output records=2803959
    
-------------------------------------------

JOB_JOIN_C0
<br/>		Map input records=2803959
<br/>		Map output records=2803959
<br/>		Reduce input records=2803959
<br/>		Reduce output records=2803959
    
-------------------------------------------

JOB_PROB_WITH_SORT
<br/>		Map input records=2803959
<br/>		Map output records=2803959
<br/>		Reduce input records=2803959
<br/>		Reduce output records=2803959

-------------------------------------------
    
    
_Master instance type:_ M1Large

_Slave instance type:_ M1Large

_Number of instances:_ 5 slaves (6 instance)
