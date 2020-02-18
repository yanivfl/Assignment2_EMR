# Assignment2_dsps
# Distributed System - Assignment 2

#### Yaniv Fleischer (yanifl) 203817002
#### Yuval Lahav (lahavyuv) 205689110

<br/><br/>
## Link to output file
https://s3.console.aws.amazon.com/s3/buckets/dsps-201-assignment2-yaniv-yuval-output/final_output/?region=us-east-1&tab=overview
 

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


<br/><br/>
## Running Instructions:
1. Create a pair key (AWS) with the name: "my_key.pem" 
2. Open 2 buckets in your S3, one for input and another for the output
3. Change in the Assignment on the Constant file the following variables:
 * INPUT_BUCKET_NAME
 * OUTPUT_BUCKET_NAME
 * MY_JAR_NAME
4. Create a jar from the assignment and upload it to the input bucket in S3 you created
5. Run the java command from the assignment:
"java -jar Assignment2.jar"
_note_: If you followed the instructions, the file should appear in your output bucket under the name: final_output


<br/><br/>
## Examples:
יש לפרש עוד	0.07399377114449149
יש לפרש זאת	0.07094469108086639
יש לפרש שני	0.06505928618216884
יש לפרש שאין	0.060363729491186346
יש לפרש לשון	0.014648859616400414

כבר היו על	0.6190800491610642
כבר היו להם	0.045087324814982815
כבר היו כמה	0.035961657114336534
כבר היו אז	0.02110635820631242
כבר היו בידי	0.012895280896035166

כדי להוכיח מה	0.15856538324491415
כדי להוכיח שהיא	0.0405226379051486
כדי להוכיח שיש	0.0339810700651614
כדי להוכיח שאני	0.02178127510981674

כגון : אף	0.05263635407794561
כגון : אתה	0.025993211464193505
כגון : שתי	0.015699914005890315
כגון : לשון	0.01418081657284413
כגון : ועוד	0.010253443709631204

כדי להוכיח מה	0.15856538324491415
כדי להוכיח שהיא	0.0405226379051486
כדי להוכיח שיש	0.0339810700651614
כדי להוכיח שאני	0.02178127510981674

על כן כדי	0.06526764501453122
על כן אחר	0.05765909452780049
על כן יום	0.05077908815594377
על כן שיש	0.03892530390377995
על כן כלל	0.037774996513148494

על על שלא	0.10895200159960274
על על אחת	0.03878719663496084
על על התורה	0.023169206672029187
על על הא	0.0

על ענייני ״	0.9765959578154769
על ענייני לשון	0.02031999945866742
על ענייני העיר	0.018926748137853527
על ענייני החברה	0.0

על פי אם	0.1682575406950786
על פי בכל	0.10829583275896909
על פי אחד	0.08486097833880862
על פי כאן	0.07887481255577143
על פי אמר	0.07498651256809481

של המדינה רק	0.1418414406854947
של המדינה מן	0.1404952742933335
של המדינה כפי	0.04135899412201727
של המדינה וגם	0.034744048975122545
של המדינה אינה	0.015656269808887444

