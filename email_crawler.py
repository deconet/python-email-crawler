from settings import LOGGING
import logging, logging.config
import urllib, urllib2
import re, urlparse
import traceback
from database import CrawlerDb
import json
from HTMLParser import HTMLParser

# Debugging
# import pdb;pdb.set_trace()

# Logging
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("crawler_logger")

google_adurl_regex = re.compile('adurl=(.*?)"')
google_url_regex = re.compile('url\?q=(.*?)&amp;sa=')
# email_regex = re.compile('([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4})', re.IGNORECASE)

# added by chris
email_regex = re.compile('(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)', re.IGNORECASE)


url_regex = re.compile('<a\s.*?href=[\'"](.*?)[\'"].*?>')
# Below url_regex will run into 'Castrophic Backtracking'!
# http://stackoverflow.com/questions/8010005/python-re-infinite-execution
# url_regex = re.compile('<a\s(?:.*?\s)*?href=[\'"](.*?)[\'"].*?>')

# Maximum number of search results to start the crawl
MAX_SEARCH_RESULTS = 150

EMAILS_FILENAME = 'data/emails.csv'
DOMAINS_FILENAME = 'data/domains.csv'

# Set up the database
db = CrawlerDb()
db.connect()


def crawl():
	"""
	This method will

	1) Google the keywords, and extract MAX_SEARCH_RESULTS
	2) For every result (aka website), crawl the website 2 levels deep.
		That is the homepage (level 1) and all it's links (level 2).
		But if level 1 has the email, then skip going to level 2.
	3) Store the html in /data/html/ and update the database of the crawled emails

	crawl(keywords):
		Extract Google search results and put all in database
		Process each search result, the webpage:
			Crawl webpage level 1, the homepage
			Crawl webpage level 2, a link away from the homepage
			Update all crawled page in database, with has_crawled = True immediately
			Store the HTML
	"""
	logger.info("-"*40)
	logger.info("Scraping from out.json")
	# logger.info("Keywords to Google for: %s" % keywords.decode('utf-8'))
	logger.info("-"*40)

	# Step 1: Crawl Google Page
	# eg http://www.google.com/search?q=singapore+web+development&start=0
	# Next page: https://www.google.com/search?q=singapore+web+development&start=10
	# Google search results are paged with 10 urls each. There are also adurls
	# for page_index in range(0, MAX_SEARCH_RESULTS, 10):
	# 	query = {'q': keywords}
	# 	url = 'http://www.google.com/search?' + urllib.urlencode(query) + '&start=' + str(page_index)
	# 	data = retrieve_html(url)
	# 	# 	print("data: \n%s" % data)
	# 	for url in google_url_regex.findall(data):
	# 		db.enqueue(unicode(url))
	# 	for url in google_adurl_regex.findall(data):
	# 		db.enqueue(unicode(url))


	# # step 1 - read all urls from json file
	with open('out-ad-and-marketing-250-999.json') as json_file:
		data = json.load(json_file)
		for p in data:
			print('Name: ' + p['name'])
			url = p['company_website_url']
			agency_name = p['name']
			print('Website: ' + url)
			print('')
			db.enqueue(unicode(url), agency_name)


	# Step 2: Crawl each of the search result
	# We search till level 2 deep
	while (True):
		# Dequeue an uncrawled webpage from db
		uncrawled = db.dequeue()
		if (uncrawled == False):
			break
		email_set = find_emails_2_level_deep(uncrawled.url, uncrawled.agency_name)
		logger.info("email_set is %s" % email_set)
		if (len(email_set) > 0):
			logger.info("marking as crawled with email_set %s" % ",".join(list(email_set)))
			db.crawled(uncrawled, ",".join(list(email_set)))
		else:
			db.crawled(uncrawled, None)

def retrieve_html(url):
	"""
	Crawl a website, and returns the whole html as an ascii string.

	On any error, return.
	"""
	req = urllib2.Request(url)
	req.add_header('User-Agent', 'Just-Crawling 0.1')
	request = None
	status = 0
	try:
		logger.info("Crawling %s" % url)
		request = urllib2.urlopen(req)
	except urllib2.URLError, e:
		try:
			logger.error("Exception at url: %s\n%s" % (url, e))
		except UnicodeDecodeError, e:
			logger.error("UnicodeDecodeError exception trying to print url")
			return
	except urllib2.HTTPError, e:
		status = e.code
	except Exception, e:
		return
	if status == 0:
		status = 200

	try:
		data = str(request.read())
	except Exception, e:
		return

	return data


def find_emails_2_level_deep(url, agency_name):
	"""
	Find the email at level 1.
	If there is an email, good. Return that email
	Else, find in level 2. Store all results in database directly, and return None
	"""
	html = retrieve_html(url)
	email_set = find_emails_in_html(html)

	mailto_email_set = set()

	if (len(email_set) > 0):
		# If there is a email, we stop at level 1.
		return email_set

	else:
		# No email at level 1. Crawl level 2
		logger.info('No email at level 1.. proceeding to crawl level 2')

		link_set, possible_mailto_email_set = find_links_in_html_with_same_hostname(url, html)
		logger.info("possible mailto empty set is %s" % possible_mailto_email_set)

		if len(possible_mailto_email_set) > 0:
			mailto_email_set |= possible_mailto_email_set
		logger.info("mailto email set is %s" % mailto_email_set)

		for link in link_set:
			# Crawl them right away!
			# Enqueue them too
			html = retrieve_html(link)
			if (html == None):
				continue
			email_set = find_emails_in_html(html)
			db.enqueue(unicode(link), agency_name, list(email_set))

		# We return a possibly empty set
		logger.info("returning mailto email set is %s" % mailto_email_set)
		return mailto_email_set


def find_emails_in_html(html):
	if (html == None):
		return set()
	email_set = set()
	for email in email_regex.findall(html):
		last_four_chars = email[-4:]
		bad_extensions = ['.jpg', '.png', '.gif', 'jpeg']
		if last_four_chars in bad_extensions:
			continue # skip this one, it's likely an image like 762x762_r-500x383@2x.jpg
		email_set.add(unicode(email))

	return email_set


def find_links_in_html_with_same_hostname(url, html):
	"""
	Find all the links with same hostname as url
	"""
	if (html == None):
		return set(), set()
	url = urlparse.urlparse(url)
	links = url_regex.findall(html)
	link_set = set()
	email_set = set()
	for link in links:
		if link == None:
			continue
		try:
			link = str(link)
			if link.endswith(".mp4"): # skip links to mp4's, lol.
				continue

			if link.startswith("/") and not link.startswith("//"):
				link_set.add('http://'+url.netloc+link)
			elif link.startswith("http") or link.startswith("https"):
				if link.find(url.netloc) and not link.find("/" + url.netloc): # filter against stuff like https://i1.wp.com/www.blairsammons.com/wp-content/uploads/2017/12/Blair_sammonsLogo.png
					link_set.add(link)
			elif link.startswith("mailto"):
				email_address = link[7:]
				if email_address.find("?") != -1:
					logger.info("Found ? in email address %s" % email_address)
					email_address = email_address[0:email_address.find("?")]
				if email_address.find("&#064") != -1:
					# we need to decode this.  &#064 is the @ symbol.
					logger.info("Found &#064 in email address %s" % email_address)
					email_address = HTMLParser().unescape(email_address)

				logger.info("Found email in mailto link: %s" % email_address)
				if len(email_address) > 0:
					email_set.add(unicode(email_address))
					logger.info("Adding email %s to list" % email_address)
			elif link.startswith("#"):
				continue
			else:
				link_set.add(urlparse.urljoin(url.geturl(),link))
		except Exception, e:
			pass

	return link_set, email_set




if __name__ == "__main__":
	import sys
	try:
		if len(sys.argv) == 1:
			crawl()
		else:
			arg = sys.argv[1].lower()
			if (arg == '--emails_and_domains') or (arg == '-ed'):
				# Get all the emails and save in a CSV
				logger.info("="*40)
				logger.info("Processing...")
				emails = db.get_all_emails_and_domains()
				logger.info("There are %d emails and domains" % len(emails))
				file = open(EMAILS_FILENAME, "w+")
				file.write("agency name,website,emails\n")
				file.writelines("\n".join(emails).encode('utf-8'))
				file.close()
				logger.info("All emails saved to ./data/emails.csv")
				logger.info("="*40)
			elif (arg == '--emails') or (arg == '-e'):
				# Get all the emails and save in a CSV
				logger.info("="*40)
				logger.info("Processing...")
				emails = db.get_all_emails()
				logger.info("There are %d emails" % len(emails))
				file = open(EMAILS_FILENAME, "w+")
				file.writelines("\n".join(emails))
				file.close()
				logger.info("All emails saved to ./data/emails.csv")
				logger.info("="*40)
			elif (arg == '--domains') or (arg == '-d'):
				# Get all the domains and save in a CSV
				logger.info("="*40)
				logger.info("Processing...")
				domains = db.get_all_domains()
				logger.info("There are %d domains" % len(domains))
				file = open(DOMAINS_FILENAME, "w+")
				file.writelines("\n".join(domains))
				file.close()
				logger.info("All domains saved to ./data/domains.csv")
				logger.info("="*40)

	except KeyboardInterrupt:
		logger.error("Stopping (KeyboardInterrupt)")
		sys.exit()
	except Exception, e:
		logger.error("EXCEPTION: %s " % e)
		traceback.print_exc()
