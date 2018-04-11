from pipeline import Pipeline, build_csv
from datetime import datetime
from nltk.corpus import stopwords
import csv
import json
import io
import string

stop_words = set(stopwords.words("english"))
pipeline = Pipeline()

@pipeline.task()
def file_to_json():
    with open("data/hacker_news_stories_2014.json", "r") as f:
        data = json.load(f)
        stories = data["stories"]
    return stories

@pipeline.task(depends_on=file_to_json)
def filter_stories(stories):
    #filter posts that have more than 50 points, 1 comment, and does not begin with 'Ask HN'
    def popular(story):
        return story["points"] > 50 and story["num_comments"] > 1 and not story["title"].startswith("Ask HN")
    
    return (story for story in stories if popular(story))

@pipeline.task(depends_on=filter_stories)
def json_to_csv(stories):
    lines = list()
    #loads json into csv files for a consistent data format
    for story in stories:
        lines.append((story["objectID"], datetime.strptime(story["created_at"], "%Y-%m-%dT%H:%M:%SZ"), story["url"], story["points"], story["title"]))
    return build_csv(lines, header=["objectID", "created_at", "url", "points", "title"], file=io.StringIO())

@pipeline.task(depends_on=json_to_csv)
def extract_titles(csv_file):
    read_file = csv.reader(csv_file)
    header = next(read_file)
    idx = header.index("title")
    
    return (line[idx] for line in read_file)

@pipeline.task(depends_on=extract_titles)
def clean_title(titles):
    for title in titles:
        title = title.lower()
        title = "".join(letters for letters in title if letters not in string.punctuation)
        yield title

@pipeline.task(depends_on=clean_title)
def build_keyword_dictionary(titles):
    freq = dict()
    for title in titles:
        for word in title.split(" "):
            if word not in stop_words:
                if word not in freq:
                    freq[word] = 1
                freq[word] += 1
    return freq

@pipeline.task(depends_on=build_keyword_dictionary)
def top_keywords(freq):
    frequency_tuple = [(word, freq[word]) for word in sorted(freq, key=freq.get, reverse=True)]
    return frequency_tuple[:100]

ran = pipeline.run()
print(ran[top_keywords])