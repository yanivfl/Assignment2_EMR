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
<br/> ש לפרש עוד	0.07399377114449149
<br/> ש לפרש זאת	0.07094469108086639
<br/> ש לפרש שני	0.06505928618216884
<br/> ש לפרש שאין	0.060363729491186346
<br/> ש לפרש לשון	0.014648859616400414
<br/> 
<br/> כבר היו על	0.6190800491610642
<br/> כבר היו להם	0.045087324814982815
<br/> כבר היו כמה	0.035961657114336534
<br/> כבר היו אז	0.02110635820631242
<br/> כבר היו בידי	0.012895280896035166
<br/> 
<br/> כדי להוכיח מה	0.15856538324491415
<br/> כדי להוכיח שהיא	0.0405226379051486
<br/> כדי להוכיח שיש	0.0339810700651614
<br/> כדי להוכיח שאני	0.02178127510981674
<br/> 
<br/> כגון : אף	0.05263635407794561
<br/> כגון : אתה	0.025993211464193505
<br/> כגון : שתי	0.015699914005890315
<br/> כגון : לשון	0.01418081657284413
<br/> כגון : ועוד	0.010253443709631204
<br/> 
<br/> כדי להוכיח מה	0.15856538324491415
<br/> כדי להוכיח שהיא	0.0405226379051486
<br/> כדי להוכיח שיש	0.0339810700651614
<br/> כדי להוכיח שאני	0.02178127510981674
<br/> 
<br/> על כן כדי	0.06526764501453122
<br/> על כן אחר	0.05765909452780049
<br/> על כן יום	0.05077908815594377
<br/> על כן שיש	0.03892530390377995
<br/> על כן כלל	0.037774996513148494
<br/> 
<br/> על על שלא	0.10895200159960274
<br/> על על אחת	0.03878719663496084
<br/> על על התורה	0.023169206672029187
<br/> על על הא	0.0
<br/> 
<br/> על ענייני ״	0.9765959578154769
<br/> על ענייני לשון	0.02031999945866742
<br/> על ענייני העיר	0.018926748137853527
<br/> על ענייני החברה	0.0
<br/> 
<br/> על פי אם	0.1682575406950786
<br/> על פי בכל	0.10829583275896909
<br/> על פי אחד	0.08486097833880862
<br/> על פי כאן	0.07887481255577143
<br/> על פי אמר	0.07498651256809481
<br/> 
<br/> של המדינה רק	0.1418414406854947
<br/> של המדינה מן	0.1404952742933335
<br/> של המדינה כפי	0.04135899412201727
<br/> של המדינה וגם	0.034744048975122545
<br/> של המדינה אינה	0.015656269808887444

