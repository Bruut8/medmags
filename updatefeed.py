#!/usr/bin/python3
# poging tot maken van python script
#

#imports
from lxml import etree, html
#import xml.etree.ElementTree as et
#from requests_html import HTMLSession
import urllib.request
import re
import MySQLdb

#vars
feeds = [
	#{'name': 'Otology_Neurotology', 'feed': 'http://ovidsp.ovid.com/rss/journals/00129492/current.rss'}
	{'name': 'Laryngoscope', 'feed': 'http://onlinelibrary.wiley.com/action/showFeed?jc=15314995&type=etoc&feed=rss'}
]
NAMESPACES = {
	'rss': 'http://purl.org/rss/1.0/',
	'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
	'prism': 'http://prismlibrary.com',
	'dc': 'http://purl.org/dc/elements/1.1/'
}
proxy = '' 


# functions
def getwebcontent(url):
	#print(' Opening url: ' + url)
	response = urllib.request.urlopen(url)
	resp = response.read()
	return resp

def getredirect(url):
	#print('  Attemping to get redirect page..')
	resp = urllib.request.urlopen(url)
	red = resp.geturl()
	#print('  Result: ' + red)
	return red

def loadxml(xml):
	xdoc = etree.fromstring(xml)
	return xdoc

def cleanxml(s):
	s = re.sub(r'<!\[CDATA\[', '', s)
	s = re.sub(r'\]\]>', '', s)
	s = re.sub(r'\n', '', s)
	s = re.sub(r'\t', '', s)
	s = re.sub(r'\r', '', s)
	str.strip(s)
	return s

def parsexml(xml, name, feed):
	print(' Attempting to drop table for journal ' + name)
	cur.execute('DROP TABLE IF EXISTS ' + name)
	print('  Succes!')
	print(' Attempting to create database for journal ' + name)
	sql_cmd = 'CREATE TABLE IF NOT EXISTS ' + name + '(title VARCHAR(400), authors VARCHAR(500), pmid INT(8), doi VARCHAR(30), volume VARCHAR(4), issue VARCHAR(3), pubdate DATE, pubtype VARCHAR(500), abstract VARCHAR(3000), pdflink VARCHAR(500))'
	cur.execute(sql_cmd)
	print('  Succes!')
	if 'ovidsp.ovid.com' in feed:
		parseovid(xml, name)
	elif 'wiley.com' in feed:
		parsewiley(xml, name)
	return

def parsewiley(xml, name):
	pmidlist = []
	print(' Identified Wiley-type feed, pubmed doi search required..')
	xdoc = loadxml(xml)
	for i in xdoc.xpath('//rdf:RDF/rss:item', namespaces=NAMESPACES):
		doi = re.sub(r'doi:', '', str(i.xpath('dc:identifier/text()', namespaces=NAMESPACES)[0]))
		pmid = pmtermsearch(doi, 'doi')
		if pmid == 0:
			print('  Skipping entry..')
		else:
			pmidlist.append(pmid)
	if len(pmidlist) > 0:
		pmidtodb(pmidlist, name)
	else:
		print(' No PMID\'s found in entire feed. Feed correct?')
	return

def parseovid(xml, name):
	xdoc = loadxml(xml)
	pmidlist = []
	print(' Identified OVID-type feed, pubmed title search required..')
	for i in xdoc.xpath('//channel/item'):
		at = cleanxml(str(i.xpath('title/text()')[0]))
		pmid = pmtermsearch(at, 'title')
		if pmid == 0:
			print('  Skipping entry..')
		else:
			pmidlist.append(pmid)
	if len(pmidlist) > 0:
		pmidtodb(pmidlist, name)
	else:
		print(' No PMID\'s found in entire feed. Feed correct?')
	return

def pmtermsearch(term, type):
	print(' Searching PubMed by ' + type + ': ' + term)
	term = re.sub(r'\s', '+', term)
	url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=' + term + '&retmode=xml&field=' + type + '&reldate=300'
	xml = getwebcontent(url)
	xdoc = loadxml(xml)
	count = int(xdoc.xpath('//eSearchResult/Count/text()')[0])
	if count == 1:
		pmid = str(xdoc.xpath('//eSearchResult/IdList/Id/text()')[0])
		print('  Succes! PMID: ' + pmid)
	else:
		pmid = 0
		print('  Fail! No results')
	return pmid

def pmidtodb(pmidlist, dbname):
	print(' Collecting info from ' + str(len(pmidlist)) + ' pmid-id\'s')
	pmid = ','.join(pmidlist)
	url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=' + pmid + '&rettype=abstract&retmode=xml'
	xml = getwebcontent(url)
	#print(xml)
	xdoc = loadxml(xml)
	for i in xdoc.xpath('//PubmedArticleSet/PubmedArticle'):
		title = str(i.xpath('MedlineCitation/Article/ArticleTitle/text()')[0])
		volume = int(i.xpath('MedlineCitation/Article/Journal/JournalIssue/Volume/text()')[0])
		issue = str(i.xpath('MedlineCitation/Article/Journal/JournalIssue/Issue/text()')[0])
		pubdate = str(i.xpath('PubmedData/History/PubMedPubDate[@PubStatus="pubmed"]/Year/text()')[0]) + '-' +  str(i.xpath('PubmedData/History/PubMedPubDate[@PubStatus="pubmed"]/Month/text()')[0]) + '-' + str(i.xpath('PubmedData/History/PubMedPubDate[@PubStatus="pubmed"]/Day/text()')[0])
		pmid = str(i.xpath('PubmedData/ArticleIdList/ArticleId[@IdType="pubmed"]/text()')[0])
		doi = str(i.xpath('PubmedData/ArticleIdList/ArticleId[@IdType="doi"]/text()')[0])
		authors = []
		for a in i.xpath('MedlineCitation/Article/AuthorList/Author'):
			authors.append(str(a.xpath('Initials/text()')[0]) + ' ' + str(a.xpath('LastName/text()')[0]))
		authors = ','.join(authors)
		abstract = []
		for a in i.xpath('MedlineCitation/Article/Abstract/AbstractText'):
			abstract.append(str(a.xpath('text()')[0]))
		abstract = '\n'.join(abstract)
		pubtype = []
		for a in i.xpath('MedlineCitation/Article/PublicationTypeList/PublicationType'):
			pubtype.append(str(a.xpath('text()')[0]))
		pubtype = ','.join(pubtype)
		#print(name,volume,issue,pubdate,pubtype,authors,pmid,doi,abstract,sep='\n')
		doired = getredirect('https://doi.org/' + doi)
		if 'Insights.ovid.com' in doired:
			#ovid weblink
			an = re.search(r'an=(.*)', doired)
			# pdflink = 'http://ovidsp.ovid.com/ovidweb.cgi?T=JS&CSC=Y&NEWS=N&PAGE=fulltext&AN=' + an.group(1) + '&LSLINK=80&D=ovft&CHANNEL=PubMed&PDF=y' 
			# Radboud:
			pdflink = 'http://ovidsp.tx.ovid.com.ru.idm.oclc.org/sp-3.31.1a/ovidweb.cgi?T=JS&CSC=Y&NEWS=N&PAGE=fulltext&AN=' + an.group(1) + '&LSLINK=80&D=ovft&CHANNEL=PubMed&PDF=y'
		elif 'wiley.com' in doired:
			#wiley weblink
			pdflink = 'https://onlinelibrary-wiley-com.ru.idm.oclc.org/doi/epdf/' + doi
		else:
			pdflink = 0
			print(' Geen PDF link kunne verkrijgen!')
		print(' Adding ' + pmid + ' to database...')
		#sql_cmd = 'INSERT INTO ' + dbname + 'VALUES(' + title + ',' + authors + ',' + pmid + ',' + doi + ',' + volume + ',' + issue + ',' + pubdate + ',' + pubtype + ',' + abstract + ',' + pdflink + ')'
		try:
			cur.execute('INSERT INTO ' + dbname + ' (title, authors, pmid, doi, volume, issue, pubdate, pubtype, abstract, pdflink) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(title,authors,pmid,doi,volume,issue,pubdate,pubtype,abstract,pdflink))
			print('  Succes!')
		except MySQLdb.Error as e:
			print(' Error: %d, %s' % (e.args[0],e.args[1]))
			input('press to continue')
	return


# MySQL connection
print(' Connecting to MySQL db...')
db = MySQLdb.connect(user='pymm', passwd='pymmpassword', db='medmags')
db.set_character_set('utf8')
cur = db.cursor()
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')
print('  Succes!')



# table
# title, authors, pmid, doi, volume, issue, pubdate, pubtype, abstract, pdflink

# start collecting

for dict in feeds:
	xml = getwebcontent(dict['feed'])
	parsexml(xml, dict['name'], dict['feed'])

cur.close()
db.commit()
exit()


