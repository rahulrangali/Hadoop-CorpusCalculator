Report on experience using amazon EMR:
•	I first logged in to Amazon Web Service , then in S3 I created a bucket with unique name. Inside the bucket I uploaded my .jar file and also created a folder inside it to store my input file that is my Corpus.txt file.
•	Then I created a cluster, In the steps part of the Cluster I added the jar file by browsing it to its path. In the arguments part I gave the path for the Corpus.txt file and also gave the path to the output directory to be created.
•	I faced a lot of problem in the AWS and had to try again and again especially the input file was not found earlier and when I gave the path, this error was solved. I also faced some problem for chaining of the output files of each map-reduce but later on solved that problem. I am using Distributed Cache in my program which caused some errors which I figured out later on.



Input-Output for the Map-Reduce:
•	I gave the Corpus.txt as input for the first Mapper which generated <word,position> as the key and value pair. On the 1st Reducer side I created count kepping <word,position> as the key and count as the value which gave me num(I,w). Output for the first Map-Reduce was<<word,position>,count>.
•	This output was given as an input using Chaining and JobClient to Mapper2 which gave the output as<postion,<word,count>> at the output. This was the input for the reducer which calculated the probability of each word at a particular position using the count value and total count. At the output of Reducer2 we get <<word,position>,probability of word>.
•	This output was taken as input by Mapper 3 which calculated the probability of sentences multiplying the probabilities of each word in a sentence. The sentence was stored in a Distributed Cache and accessed. And the top 3 sentences are displayed at the final output.
