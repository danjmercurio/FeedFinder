#!/usr/bin/env python

import os
import bs4
import requests
import time
import urlparse
import os
import sys
import MySQLdb as db
import re
from collections import OrderedDict
import feedparser

# ignore warnings about pre-existing tables in sql
from warnings import filterwarnings
filterwarnings('ignore', category = db.Warning)


class Finder(object):

	def __init__(self,startURL,keyword):
		self.startURL = startURL
		self.keyword = keyword
		if not (self.startURL.lower().startswith("http://") or self.startURL.lower().startswith("https://")):
			self.startURL = "http://" + self.startURL
		self.attributes = [
							'application/rss+xml',
							'application/atom+xml',
							'application/rss',
							'application/atom',
							'application/rdf+xml',
							'application/rdf',
							'text/rss+xml',
							'text/atom+xml',
							'text/rss',
							'text/atom',
							'text/rdf+xml',
							'text/rdf',
							'text/xml',
							'application/xml'
							]
		self.discoveredFeeds = []
		self.toSearch = []
		self.searched = [] # load in from sql
		self.SQLfeeds = []

		self.toSearch.append(self.startURL)


		if (os.path.exists('sql.cfg')):
			# filter out comments
			with open('sql.cfg','r') as sqlfile:
				sqlconfig = [x.strip() for x in sqlfile.readlines()]
				sqlconfig = filter(lambda i: not i.startswith('#'), sqlconfig)

				self.sql_Host = sqlconfig [0]
				self.sql_User = sqlconfig[1]
				self.sql_Password = sqlconfig[2]
				self.sql_DB = sqlconfig[3]
		else:
			print "No sql.cfg file. Check that this file exists, and is in the same folder as ff.py"
			sys.exit()

		try:
			self.conn = db.connect(self.sql_Host,self.sql_User,self.sql_Password)
			print "Connected to MySQL server on " + self.sql_Host + " as " + self.sql_User
			with self.conn:
				self.cursor = self.conn.cursor()
				
				self.cursor.execute("CREATE DATABASE IF NOT EXISTS " + self.sql_DB + ";");
				self.cursor.execute("USE " + self.sql_DB + ";")

				#create the table if it doesn't exist with the following structure
				createstmt = 'CREATE TABLE IF NOT EXISTS `feeds` (  `id` int(11) NOT NULL AUTO_INCREMENT,  `href` varchar(250) NOT NULL,  `title` varchar(250) NOT NULL,  PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;'
				self.cursor.execute(createstmt)

				#load all previous feeds into self.discoveredFeeds

				self.cursor.execute("SELECT * FROM feeds")

				rows = self.cursor.fetchall()

				for row in rows:
					newdict = {'href':row[1],'title':row[2]}
					self.SQLfeeds.append(newdict)



		except (db.Error):
			print "Unable to connect to SQL. Check sql.cfg and MySQL installation."
			sys.exit()
		
		

	
	def addFeed(self, item):
		with self.conn:
			insertString = 'INSERT INTO `feeds`(`id`,`href`,`title`) VALUES (NULL,\'' + item.get('href') + '\',\'' + item.get('title') + '\');'
			self.cursor.execute(insertString)
			print "Inserted feed at " + item.get('href') + " to mysql."


	def checkRSS(self,soup,page):
		# does it contain a reference to a feed in the head tag?
		for link in soup.findAll('link', href=True):
			href = link.attrs['href']			
			# weed out id tags, jpegs, and javascript links
			if self.allow(href):
				try:
					if link.attrs['type'] in self.attributes:
						print "FOUND RSS FEED!"
						if not (href.startswith('http://') or href.startswith('https://')):
							href = urlparse.urljoin(page.url, href)
						print href
						try:
							title = link.attrs['title']
						except KeyError:
							title = link.attrs['type']
						return {'href': href, 'title': title}
				except KeyError:
					pass
		# is it a feed?
		try:	
			f = feedparser.parse(page.text)
			if len(f.version) > 0:
				print "FOUND RSS FEED!"
				if not (href.startswith('http://') or href.startswith('https://')):
					href = urlparse.urljoin(page.url, href)
				print href
				return {'href': href, 'title': page.url}
			else:
				return False
		except:
			return False

		


	def fetch(self,url):
		if self.allow(url):
			try:	
				print 'Get: ' + url + '...',
				starttime = time.clock()
				result = requests.get(url)
				
				if result.ok:
					print "done in " + str((time.clock() - starttime) * 1000) + " ms"
					return result
				elif result.status_code == 429:
					print "Too many requests. Waiting 10 seconds to continue..."
					time.sleep(10.0)
					return self.fetch(url)
				else:
					print "Server returned error " + str(result.status_code)
					return False
			except (requests.exceptions.ConnectionError):
				print "Connection refused"
				return False
			except (requests.exceptions.HTTPError):
				print "Malformed request"
				return False
			except (requests.exceptions.TooManyRedirects):
				print "Encountered redirect loop. Skipping."
				return False
			except (requests.exceptions.Timeout):
				print "Request timed out"
				return False
			except (requests.exceptions.URLRequired):
				print "Malformed url"
				return False
			except (requests.exceptions.InvalidSchema):
				print "Malformed url"
				return False
			except (requests.exceptions.MissingSchema):
				return self.fetch('http://' + url)
			# a generic exception
		else:
			return False

	# a boolean to weed out links we don't want
	def allow(self, href):
		href = href.lower()
		return not (href.startswith('mailto:') or href.startswith('#') or href.endswith('.jpg') or href.endswith('.gif') or href.endswith('.png') or href.startswith('javascript') or href.endswith(".pdf") or href.endswith(".css") or href.endswith(".ico"))
		
	# get all of the links from a page
	def extractLinks(self, soup, page):
		extractedLinks = []
		# for each a tag with a href attribute, do...
		for link in soup.findAll('a', href=True):
			href = link.attrs['href']
			# weed out id tags, jpegs, and javascript links
			if self.allow(href):
			# only append if we have the full path, if not, join it with urljoin
				if (href.startswith('http://') or href.startswith('https://') or href.startswith('//')):
					extractedLinks.append(href)
				else:
					extractedLinks.append(urlparse.urljoin(page.url,href))
		# remove any duplicates on the page
		extractedLinks = OrderedDict.fromkeys(extractedLinks).keys()
		# strip all trailing slashes
		extractedLinks = map(lambda x: x.strip('/'), extractedLinks)
		return extractedLinks

	def crawl(self):
		try:
			while (True): # keep looping
				for link in self.toSearch: #for each link in 'to search' list
					if (link not in self.searched): # if we have not already searched it, do...
						# fetch the content
						page = self.fetch(link)
						
						# if the fetch worked
						if page:
							# make a soup
							soup = bs4.BeautifulSoup(page.text)
							# analyze for feeds
							feed = self.checkRSS(soup, page)
							if feed:
								# we found a feed. add it to mysql.
								self.addFeed(feed)					
							# get all of this page's links
							links = self.extractLinks(soup, page)
							for pagelink in links:
								# for each link, if it's not already in 'searched' add to 'to search'
								# also check it against 'to search' to avoid duplicates
								if pagelink not in self.searched and pagelink not in self.toSearch:
									self.toSearch.append(pagelink)
							print "Found " + str(len(links)) + " links."
							# add it to the links already searched
							self.searched.append(link.strip('/'))
							# remove it from links to search
							indextoRemove = self.toSearch.index(link)
							del self.toSearch[indextoRemove]
							del soup

			print "Ran out of URLs to crawl. This shouldn't be happening!"
			sys.exit(0)

		except (KeyboardInterrupt):
			
			print "Crawling halted."
			


if __name__=='__main__':
	print
	print "feedFinder 0.1 by Dan Mercurio"
	print "Enter a keyword to filter results, or just press enter for all results"
	keyword = raw_input()

	if (os.path.exists('urls.txt')):
		print "urls.txt found. Reading URLs from file..."
		urlfile = open('urls.txt','r').readlines()
		urls = map(lambda x: x.strip(), urlfile)
		startURL = urls.pop(0)

		f = Finder(startURL,keyword)
		for i in urls:
			f.toSearch.append(i.strip('/'))
		f.crawl()
	else:
		print "Enter a site to start crawling and press enter. Press CTRL-C to halt."
		startURL = raw_input()
		if (startURL == ''):
			startURL = 'http://reddit.com/'
		f = Finder(startURL,keyword).crawl()

