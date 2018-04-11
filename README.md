# Hacker News Pipeline

This is an ongoing project that analyzes stories from Hacker News. The data is from the [Hacker News API](https://hn.algolia.com/api) that returns a JSON dataset format of the top stories in 2014. I initially started the project as a practice to build the DAG pipeline and to output CSV files within the pipeline.

The results display the top 100 keywords in each story. There were terms like bitcoin (the cryptocurrency), heartbleed (a security bug exploit), and many others. Even though this was a fundamental natural language processing task, it did provide some interesting insights into conversations from 2014. 

What's next? 
- Rewrite the entire pipeline using [Airflow](http://airbnb.io/projects/airflow/).
- Covert the information into a CSV before filtering and output a raw file. 
- Use an advanced method of language processing through the [nltk package](http://www.nltk.org/).