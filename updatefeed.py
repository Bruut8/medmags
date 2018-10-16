#!/usr/bin/python3
# poging tot maken van python script
#

#imports
from lxml import etree
import urllib.request
import re
import MySQLdb

#vars
NAMESPACES = {
	'rss': 'http://purl.org/rss/1.0/',
	'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
	'prism': 'http://purl.org/rss/1.0/modules/prism/',
	'dc': 'http://purl.org/dc/elements/1.1/'
}



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
	if 'ovidsp.ovid.com' in feed:
		parseovid(xml, name)
	elif 'wiley.com' in feed:
		parsewiley(xml, name)
	elif 'jamanetwork.com' in feed:
		parsejama(xml, name)
	return

def resettable(name):
	print(' Attempting to drop table for journal ' + name)
	cur.execute('DROP TABLE IF EXISTS ' + name)
	print('  Succes!')
	print(' Attempting to create table for journal ' + name)
	sql_cmd = 'CREATE TABLE IF NOT EXISTS ' + name + '(title VARCHAR(400), authors VARCHAR(500), pmid INT(8), doi VARCHAR(30), volume VARCHAR(4), issue VARCHAR(3), pubdate DATE, pubtype VARCHAR(500), abstract VARCHAR(3000), pdflink VARCHAR(500), weblink VARCHAR(500))'
	cur.execute(sql_cmd)
	print('  Succes!')
	
def checkuptodate(name, xmlvolume, xmlissue):
	# Get vol and issue from database
	cur.execute('SELECT volume,issue FROM shelf WHERE journal = %s', (name,))
	rows = cur.fetchall()
	for r in rows:
		voldb = str(r[0])
		issdb = str(r[1])
		print(name + ' last updated to volume ' + voldb + ' issue ' + issdb)
	# compare and if match then skip
	if xmlvolume == voldb and xmlissue == issdb:
		print(' Already up to date, skipping ' + name)
		return 1
	else:
		return 0

def strip_ns_prefix(tree):
    #xpath query for selecting all element nodes in namespace
    query = "descendant-or-self::*[namespace-uri()!='']"
    #for each element returned by the above xpath query...
    for element in tree.xpath(query):
        #replace element name with its local name
        element.tag = etree.QName(element).localname
    return tree		
		
def parsejama(xml, name):
	xdoc = loadxml(xml)
	pmidlist = []
	# prism needed so remove namespaces
	xdoc = strip_ns_prefix(xdoc)
	# Get vol and issue from xml
	volume = str(xdoc.xpath('//rss/channel/item/volume/text()')[0])
	issue = str(xdoc.xpath('//rss/channel/item/number/text()')[0])
	print(' XML version: Volume ' + volume + ' Issue ' + issue)
	if checkuptodate(name, volume, issue) == 1:
		return
	# if not up-to-date then drop table
	print(' Identified JAMA-type feed, pubmed doi search required..')
	resettable(name)
	for i in xdoc.xpath('//rss/channel/item'):
		doi = cleanxml(str(i.xpath('doi/text()')[0]))
		pmid = pmtermsearch(doi, 'doi')
		if pmid == 0:
			print('  Skipping entry..')
		else:
			pmidlist.append(pmid)
	if len(pmidlist) > 0:
		pmidtodb(pmidlist, name)
		print(' Updating shelf...')
		# Update shelf
		cur.execute('UPDATE shelf SET volume = %s, issue = %s WHERE journal = %s',(volume, issue, name))
	else:
		print(' No PMID\'s found in entire feed. Feed correct?')
	return
	
def parsewiley(xml, name):
	xdoc = loadxml(xml)
	pmidlist = []
	# Get vol and issue from xml
	t = re.search('Volume\s+(\d+).*Issue\s+(\d+)',cleanxml(str(xdoc.xpath('//rdf:RDF/rss:item/rss:description/text()', namespaces=NAMESPACES)))) 
	volume = t.group(1)
	issue = t.group(2)
	print(' XML version: Volume ' + volume + ' Issue ' + issue)
	if checkuptodate(name, volume, issue) == 1:
		return
	# if not up-to-date then drop table
	print(' Identified Wiley-type feed, pubmed doi search required..')
	resettable(name)
	for i in xdoc.xpath('//rdf:RDF/rss:item', namespaces=NAMESPACES):
		doi = re.sub(r'doi:', '', str(i.xpath('dc:identifier/text()', namespaces=NAMESPACES)[0]))
		pmid = pmtermsearch(doi, 'doi')
		if pmid == 0:
			print('  Skipping entry..')
		else:
			pmidlist.append(pmid)
	if len(pmidlist) > 0:
		pmidtodb(pmidlist, name)
		print(' Updating shelf...')
		# Update shelf
		cur.execute('UPDATE shelf SET volume = %s, issue = %s WHERE journal = %s',(volume, issue, name))
	else:
		print(' No PMID\'s found in entire feed. Feed correct?')
	return

def parseovid(xml, name):
	xdoc = loadxml(xml)
	pmidlist = []
	# Get vol and issue from xml
	t = re.search('Volume\s+(\d+)\((\d+)\)',cleanxml(str(xdoc.xpath('//channel/title/text()')))) 
	volume = t.group(1)
	issue = t.group(2)
	print(' XML version: Volume ' + volume + ' Issue ' + issue)
	if checkuptodate(name, volume, issue) == 1:
		return	
	# if not skipped then drop table	
	resettable(name)
	print(' Identified OVID-type feed, pubmed title search required..')
	# Get titles from xml 
	for i in xdoc.xpath('//channel/item'):
		at = cleanxml(str(i.xpath('title/text()')[0]))
		# Convert titles to pmids
		pmid = pmtermsearch(at, 'title')
		if pmid == 0:
			print('  Skipping entry..')
		else:
			pmidlist.append(pmid)
	# If titles are converted to pmids
	if len(pmidlist) > 0:
		pmidtodb(pmidlist, name)
		print(' Updating shelf...')
		# Update shelf
		cur.execute('UPDATE shelf SET volume = %s, issue = %s WHERE journal = %s',(volume, issue, name))
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
		try:
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
			if not abstract:
				abstract = "No abstract available"
			pubtype = []
			for a in i.xpath('MedlineCitation/Article/PublicationTypeList/PublicationType'):
				pubtype.append(str(a.xpath('text()')[0]))
			pubtype = ','.join(pubtype)
			#print(name,volume,issue,pubdate,pubtype,authors,pmid,doi,abstract,sep='\n')
			doired = getredirect('https://doi.org/' + doi)
			if 'Insights.ovid.com' in doired:
				#ovid pdflink
				an = re.search(r'an=(.*)', doired)
				# pdflink = 'http://ovidsp.ovid.com/ovidweb.cgi?T=JS&CSC=Y&NEWS=N&PAGE=fulltext&AN=' + an.group(1) + '&LSLINK=80&D=ovft&CHANNEL=PubMed&PDF=y' 
				# Radboud:
				pdflink = 'http://ovidsp.tx.ovid.com.ru.idm.oclc.org/sp-3.31.1a/ovidweb.cgi?T=JS&CSC=Y&NEWS=N&PAGE=fulltext&AN=' + an.group(1) + '&LSLINK=80&D=ovft&CHANNEL=PubMed&PDF=y'
				#ovid weblink
				weblink = 'http://ovidsp.tx.ovid.com.ru.idm.oclc.org/sp-3.31.1a/ovidweb.cgi?T=JS&CSC=Y&NEWS=N&PAGE=fulltext&AN=' + an.group(1) + '&LSLINK=80&D=ovft&CHANNEL=PubMed&PDF=n'
			elif 'wiley.com' in doired:
				#wiley pdflink
				pdflink = 'https://onlinelibrary-wiley-com.ru.idm.oclc.org/doi/epdf/' + doi
				#wiley weblink
				weblink = 'https://onlinelibrary-wiley-com.ru.idm.oclc.org/doi/full/' + doi
			elif 'jamanetwork' in doired:
				#jama pdflink
				jour = re.search(r'journals/(\w+)/article-abstract', doired)
				id = re.search(r'article-abstract/(\d+)', doired)
				redweb = str(getwebcontent(doired))
				dau = re.search(r'data-article-url=\"(.*?)\"', redweb)
				print(' Redirected to ' + doired)
				print('  Found journal: ' + jour.group(1))
				print('  Found article-id: ' + id.group(1))
				print('  Found dau: ' + dau.group(1))
				pdflink = 'https://jamanetwork-com.ru.idm.oclc.org' + dau.group(1)
				weblink = 'https://jamanetwork-com.ru.idm.oclc.org/journals/' + jour.group(1) + '/fullarticle/' + id.group(1)
			else:
				pdflink = 0
				weblink = 0
				print(' Geen PDF- of Weblink kunnen verkrijgen!')
			print(' Adding ' + pmid + ' to database...')
			#sql_cmd = 'INSERT INTO ' + dbname + 'VALUES(' + title + ',' + authors + ',' + pmid + ',' + doi + ',' + volume + ',' + issue + ',' + pubdate + ',' + pubtype + ',' + abstract + ',' + pdflink + ')'
			try:
				cur.execute('INSERT INTO ' + dbname + ' (title, authors, pmid, doi, volume, issue, pubdate, pubtype, abstract, pdflink, weblink) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(title,authors,pmid,doi,volume,issue,pubdate,pubtype,abstract,pdflink,weblink))
				print('  Succes!')
			except MySQLdb.Error as e:
				print(' Error: %d, %s' % (e.args[0],e.args[1]))
				input('press to continue')
		except IndexError:
			pass
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

# Check update dates
print(' Check Magazine collection...')
sql_cmd = 'CREATE TABLE IF NOT EXISTS shelf (journal VARCHAR(400), feed VARCHAR(500), last_update DATE)'
cur.execute(sql_cmd)
cur.execute('SELECT * FROM shelf WHERE last_update <> CURDATE() OR last_update IS NULL')
rows = cur.fetchall()
for r in rows:
	print(' Updating ' + r[0])
	xml = getwebcontent(r[1])
	parsexml(xml, r[0], r[1])
	cur.execute('UPDATE shelf SET last_update = CURDATE() WHERE journal = %s',(r[0],))
cur.close()
db.commit()
exit()


# table
# title, authors, pmid, doi, volume, issue, pubdate, pubtype, abstract, pdflink, weblink

# shelf table
# journal, feed, last_update, volume, issue
