prerequisites: sbt
sbt is easy to install by using homebrew 
installation instructions at (http://brew.sh/)
brew install sbt

steps to run:
1. cd into word-frequency directory
2. sbt
3. console
4. import org.wordfrequency.WordFrequency
5. WordFrequency.main(args = Array("queries2.txt", "records2.txt", "output2.txt"))

Word Frequency

Consider a list of sets of words (each set of words is called a record) and a single set of words (called the query). For each word that is not in the query, how many times does the word appear in all records that the entire query is present in? Output a dictionary of words to counts, omitting words with counts of zero. Given a list of records and a list of queries, determine the output for each query with respect to the entire list of records.
Example

The following are examples of a records file and queries file, and the expected output from your solution.
records.txt

red,yellow,green,black
red,green,blue,black
yellow,green,blue
yellow,blue,black
queries.txt

blue,yellow
black,green
expected output

{"black": 1, "green": 1}
{"blue": 1, "red": 2, "yellow": 1}

Challenge

There are two input files: records.txt and queries.txt. The records file contains a large list of sets of words to be used as training data. The queries file contains individual sets of words to be interpreted as queries against all of the records.
Each file consists of ASCII-encoded text.
Each line of the files contains a set of comma-separated words.
Aside from the separating commas, there is no other punctuation in the files.
None of the words contain whitespace.
Words are case-sensitive.
No word appears more than once in the same line.
