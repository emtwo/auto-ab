import json
from bs4 import BeautifulSoup

html_doc = open("dashboard_template.html", "r+")
soup = BeautifulSoup(html_doc, 'html.parser')

with open("graph_links") as graphs:
  graph_links = graphs.read()

iframes = soup.find_all('iframe')
graph_links = json.loads(graph_links)
for i in xrange(len(iframes)):
  new_tag = soup.new_tag("iframe", src=graph_links[i])
  iframes[i].replace_with(new_tag)

html_doc.close()

html = soup.prettify(soup.original_encoding)
with open("test.html", "wb") as file:
  file.write(html)
