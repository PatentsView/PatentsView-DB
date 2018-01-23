def parse_patents(fd,fd2):
	import re,csv,os,codecs,zipfile,traceback
	import string,random,HTMLParser
	
	def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
		return ''.join(random.choice(chars) for _ in range(size))
	
	type_kind = {'1': ["A","utility"],
				 '2': ["E","reissue"],
				 '3': ["I5","TVPP"],
				 '4': ["S","design"],
				 '5': ["I4","defensive publication"],
				 '6': ["P","plant"],
				 '7': ["H","statutory invention registration"]
				 }    
	
	reldoctype = [
				'continuation-in-part',
				'continuation_in_part',
				'continuing_reissue',
				'division',
				'reissue',
				'related_publication',
				'substitution',
				'us_provisional_application',
				'us_reexamination_reissue_merger',
				'continuation'
				]
	
	fd+='/'
	fd2+='/'
	diri = os.listdir(fd)
	diri = [d for d in diri if d.endswith('zip')]

	#Initiate HTML Parser for unescape characters
	h = HTMLParser.HTMLParser()

	#Remove all files from output dir before writing
	outdir = os.listdir(fd2)
	for oo in outdir:
		os.remove(os.path.join(fd2,oo))

	#Rewrite files and write headers to them
	appfile = open(os.path.join(fd2,'application.csv'),'wb')
	appfile.write(codecs.BOM_UTF8)
	app = csv.writer(appfile,delimiter='\t')
	app.writerow(['id','patent_id','type','number','country','date'])
	
	claimsfile = open(os.path.join(fd2,'claim.csv'),'wb')
	claimsfile.write(codecs.BOM_UTF8)
	clms = csv.writer(claimsfile,delimiter='\t')
	clms.writerow(['uuid','patent_id','text','dependent','sequence', 'exemplary'])
	
	rawlocfile = open(os.path.join(fd2,'rawlocation.csv'),'wb')
	rawlocfile.write(codecs.BOM_UTF8)
	rawloc = csv.writer(rawlocfile,delimiter='\t')
	rawloc.writerow(['id','location_id','city','state','country','zip_code'])
	
	rawinvfile = open(os.path.join(fd2,'rawinventor.csv'),'wb')
	rawinvfile.write(codecs.BOM_UTF8)
	rawinv = csv.writer(rawinvfile,delimiter='\t')
	rawinv.writerow(['uuid','patent_id','inventor_id','rawlocation_id','name_first','name_last',"other_info",'sequence',"rule_47"])
	
	rawassgfile = open(os.path.join(fd2,'rawassignee.csv'),'wb')
	rawassgfile.write(codecs.BOM_UTF8)
	rawassg = csv.writer(rawassgfile,delimiter='\t')
	rawassg.writerow(['uuid','patent_id','assignee_id','rawlocation_id','type','name_first','name_last','organization',"other_info",'sequence'])
	
	ipcrfile = open(os.path.join(fd2,'ipcr.csv'),'wb')
	ipcrfile.write(codecs.BOM_UTF8)
	ipcr = csv.writer(ipcrfile,delimiter='\t')
	ipcr.writerow(['uuid','patent_id','classification_level','section','mainclass','subclass','main_group','subgroup','symbol_position','classification_value','classification_status','classification_data_source','action_date','ipc_version_indicator','sequence'])
	
	patfile = open(os.path.join(fd2,'patent.csv'),'wb')
	patfile.write(codecs.BOM_UTF8)
	pat = csv.writer(patfile,delimiter='\t')
	pat.writerow(['id','type','number','country','date','abstract','title','kind','num_claims', 'filename'])
	
	uspatentcitfile = open(os.path.join(fd2,'uspatentcitation.csv'),'wb')
	uspatentcitfile.write(codecs.BOM_UTF8)
	uspatcit = csv.writer(uspatentcitfile,delimiter='\t')
	uspatcit.writerow(['uuid','patent_id','citation_id','date','name','kind','country','category','sequence'])
	
	foreigncitfile = open(os.path.join(fd2,'foreigncitation.csv'),'wb')
	foreigncitfile.write(codecs.BOM_UTF8)
	foreigncit = csv.writer(foreigncitfile,delimiter='\t')
	foreigncit.writerow(['uuid','patent_id','date','number','country','category','sequence'])
	
	otherreffile = open(os.path.join(fd2,'otherreference.csv'),'wb')
	otherreffile.write(codecs.BOM_UTF8)
	otherref = csv.writer(otherreffile,delimiter='\t')
	otherref.writerow(['uuid','patent_id','text','sequence'])
	
	examfile = open(os.path.join(fd2,'examiner.csv'),'wb')
	examfile.write(codecs.BOM_UTF8)
	examiner = csv.writer(examfile,delimiter='\t')
	examiner.writerow(['id','patent_id','fname','lname','role','group'])
	
	rawlawyerfile = open(os.path.join(fd2,'rawlawyer.csv'),'wb')
	rawlawyerfile.write(codecs.BOM_UTF8)
	rawlawyer = csv.writer(rawlawyerfile,delimiter='\t')
	rawlawyer.writerow(['uuid','lawyer_id','patent_id','name_first','name_last','organization','country','sequence'])
	
	uspcfile = open(os.path.join(fd2,'uspc.csv'),'wb')
	uspcfile.write(codecs.BOM_UTF8)
	uspcc = csv.writer(uspcfile,delimiter='\t')
	uspcc.writerow(['uuid','patent_id','mainclass_id','subclass_id','sequence'])

	mainclassfile = open(os.path.join(fd2,'mainclass.csv'),'wb')
	mainclassfile.write(codecs.BOM_UTF8)
	mainclass = csv.writer(mainclassfile,delimiter='\t')
	mainclass.writerow(['id'])

	subclassfile = open(os.path.join(fd2,'subclass.csv'),'wb')
	subclassfile.write(codecs.BOM_UTF8)
	subclass = csv.writer(subclassfile,delimiter='\t')
	subclass.writerow(['id'])
	
	### New fields ###
	forpriorityfile = open(os.path.join(fd2,'foreign_priority.csv'),'wb')
	forpriorityfile.write(codecs.BOM_UTF8)
	forpriority = csv.writer(forpriorityfile,delimiter='\t')
	forpriority.writerow(['uuid', 'patent_id', "sequence", "kind", "app_num", "app_date", "country"])

	##### BEGIN PARENT CASE logical group is the USRELDOC #####

	usreldocfile = open(os.path.join(fd2,'usreldoc.csv'), 'wb')
	usreldocfile.write(codecs.BOM_UTF8)
	usrel = csv.writer(usreldocfile, delimiter='\t')
	usrel.writerow(['uuid', 'patent_id', 'doc_type',  'relkind', 'reldocno', 'relcountry', 'reldate',  'parent_status', 'rel_seq','kind'])

	##### END PARENT CASE logical group is the USRELDOC #####
	
	us_term_of_grantfile = open(os.path.join(fd2,'us_term_of_grant.csv'), 'wb')
	us_term_of_grantfile.write(codecs.BOM_UTF8)
	us_term_of_grant = csv.writer(us_term_of_grantfile, delimiter='\t')
	us_term_of_grant.writerow(['uuid','patent_id','lapse_of_patent', 'disclaimer_date' 'term_disclaimer', 'term_grant', 'term_ext'])

	draw_desc_textfile = open(os.path.join(fd2,'draw_desc_text.csv'), 'wb')
	draw_desc_textfile.write(codecs.BOM_UTF8)
	drawdesc = csv.writer(draw_desc_textfile, delimiter='\t')
	drawdesc.writerow(['uuid', 'patent_id', 'text', 'seq'])

	brf_sum_textfile = open(os.path.join(fd2,'brf_sum_text.csv'), 'wb')
	brf_sum_textfile.write(codecs.BOM_UTF8)
	brf_sum = csv.writer(brf_sum_textfile, delimiter='\t')
	brf_sum.writerow(['uuid', 'patent_id', 'text'])

	det_desc_textfile = open(os.path.join(fd2,'detail_desc_text.csv'), 'wb')
	det_desc_textfile.write(codecs.BOM_UTF8)
	det_desc = csv.writer(det_desc_textfile, delimiter='\t')
	det_desc.writerow(['uuid', 'patent_id', 'text', 'length'])

	rel_app_textfile = open(os.path.join(fd2,'rel_app_text.csv'), 'wb')
	rel_app_textfile.write(codecs.BOM_UTF8)
	rel_app = csv.writer(rel_app_textfile, delimiter='\t')
	rel_app.writerow(['uuid', 'patent_id',"text"])

	non_inventor_applicantfile = open(os.path.join(fd2,'non_inventor_applicant.csv'),'wb')
	non_inventor_applicantfile.write(codecs.BOM_UTF8)
	noninventorapplicant = csv.writer(non_inventor_applicantfile,delimiter='\t')
	noninventorapplicant.writerow(['uuid', 'patent_id', "location_id", "last_name", "first_name", "org_name", "sequence", "designation", "applicant_type"])

	pct_datafile = open(os.path.join(fd2,'pct_data.csv'), 'wb')
	pct_datafile.write(codecs.BOM_UTF8)
	pct_data = csv.writer(pct_datafile, delimiter='\t')
	pct_data.writerow(['uuid', 'patent_id', 'rel_id', 'date', '371_date', 'country', 'kind', "doc_type","102_date"])

	botanicfile = open(os.path.join(fd2,'botanic.csv'), 'wb')
	botanicfile.write(codecs.BOM_UTF8)
	botanic_info = csv.writer(botanicfile, delimiter='\t')
	botanic_info.writerow(['uuid', 'patent_id', 'latin_name', "variety"])

	figurefile = open(os.path.join(fd2,'figures.csv'), 'wb')
	figurefile.write(codecs.BOM_UTF8)
	figure_info = csv.writer(figurefile, delimiter='\t')
	figure_info.writerow(['uuid', 'patent_id', 'num_figs', "num_sheets"])   
	
	mainclassfile.close()
	subclassfile.close()
	appfile.close()
	rawlocfile.close()
	rawinvfile.close()
	rawassgfile.close()
	ipcrfile.close()
	otherreffile.close()
	foreigncitfile.close()
	patfile.close()
	rawlawyerfile.close()
	uspatentcitfile.close()
	uspcfile.close()
	claimsfile.close()
	examfile.close()
	forpriorityfile.close()
	us_term_of_grantfile.close()
	usreldocfile.close()
	non_inventor_applicantfile.close()
	draw_desc_textfile.close()
	brf_sum_textfile.close()
	rel_app_textfile.close()
	det_desc_textfile.close()
	pct_datafile.close()
	botanicfile.close()
	figurefile.close()
	
	
	loggroups = ['PATN','INVT','ASSG','PRIR','REIS','RLAP','CLAS','UREF','FREF','OREF','LREP','PCTA','ABST','GOVT','PARN','BSUM','DRWD','DETD','CLMS','DCLM']
	
	numii = 0
	rawlocation = {}
	mainclassdata = {}
	subclassdata = {}

	for d in diri:
	  print d
	  inp = zipfile.ZipFile(os.path.join(fd,d))
	  for i in inp.namelist():
		infile = h.unescape(inp.open(i).read().decode('utf-8','ignore').replace('&angst','&aring')).replace("\r","").split('PATN')
		del infile[0]

		for i in infile:
			numii+=1
			try:    
				i = i.encode('utf-8','ignore')
				# Get relevant logical groups from patent records according to documentation
				# Some patents can contain several INVT, ASSG and other logical groups - so, is important to retain all
				avail_fields = {}
				num = 1
				avail_fields['PATN'] = i.split('INVT')[0]
				runnums = []
				for n in range(1,len(loggroups)):
					try:
						gg = re.search('\n'+loggroups[n],i).group()
						if num-n == 0:
							runnums.append(n)
							num+=1
							go = list(re.finditer('\n'+loggroups[n-1],i))
							if len(go) == 1:
								needed = i.split(loggroups[n-1])[1]
								avail_fields[loggroups[n-1]] = needed.split(loggroups[n])[0]
							elif len(go) > 1:
								needed = '\n\n\n\n\n'.join(i.split(loggroups[n-1])[1:])
								avail_fields[loggroups[n-1]] = needed.split(loggroups[n])[0]
							else:
								pass
						else:
							go = list(re.finditer('\n'+loggroups[runnums[-1]],i))
							if len(go) == 1:
								needed = i.split(loggroups[runnums[-1]])[1]
								avail_fields[loggroups[runnums[-1]]] = needed.split(loggroups[n])[0]
							elif len(go) > 1:
								needed = '\n\n\n\n\n'.join(i.split(loggroups[runnums[-1]])[1:])
								avail_fields[loggroups[runnums[-1]]] = needed.split(loggroups[n])[0]
							else:
								pass
							runnums.append(n)
							num = n+1
						
					except:
						pass
				# Create containers based on existing Berkeley DB schema (not all are currently used - possible compatibility issues)
				application = {}
				claimsdata = {}
				examiner = {}
				foreigncitation = {}
				ipcr = {}
				otherreference = {}
				patentdata = {}
				pctdata = {}
				prioritydata = {}
				rawassignee = {}
				rawinventor = {}
				rawlawyer = {}
				usappcitation = {}
				uspatentcitation = {}
				uspc = {}
				usreldoc = {}
				figureinfo = {}
				termofgrant = {}
				drawdescdata = {}
				detail_desc_text = {}
				relappdata = {}
				
				###                PARSERS FOR LOGICAL GROUPS                  ###

				

				try:
					numfigs = ''
					numsheets = ''
					disclaimerdate=''
					termpat = ''
					patent = avail_fields['PATN'].split('\n')
					for line in patent:
						if line.startswith("WKU"):
							patnum = re.search('WKU\s+(.*?)$',line).group(1)
							updnum = re.sub('^H0','H',patnum)[:8]
							updnum = re.sub('^RE0','RE',updnum)[:8]
							updnum = re.sub('^PP0','PP',updnum)[:8]
							updnum = re.sub('^PP0','PP',updnum)[:8]
							updnum = re.sub('^D0', 'D', updnum)[:8]
							updnum = re.sub('^T0', 'T', updnum)[:8]
							if len(patnum) > 7 and patnum.startswith('0'):
								updnum = patnum[1:8]
							patent_id = updnum
						if line.startswith('SRC'):
							seriescode = re.search('SRC\s+(.*?)$',line).group(1)
							try:
								gg = int(seriescode)
								if len(seriescode) == 1:
									seriescode = '0'+seriescode
							except:
								pass
						if line.startswith('APN'):
							appnum = re.search('APN\s+(.*?)$',line).group(1)[:6]
							if len(appnum) != 6:
								appnum = 'NULL'
								#data['appnum'] = appnum
						if line.startswith('APT'):
							apptype = re.search('APT\s+(.*?)$',line).group(1)
							apptype = re.search('\d',apptype).group()
						if line.startswith('APD'):
							appdate = re.search('APD\s+(.*?)$',line).group(1)
							appdate = appdate[:4]+'-'+appdate[4:6]+'-'+appdate[6:]
						if line.startswith('TTL'):
							title = re.search('TTL\s+(.*?)ISD',avail_fields['PATN'],re.DOTALL).group(1)
							title = re.sub('[\n\t\r\f]+','',title)
							title = re.sub('\s+$','',title)
							title = re.sub('\s+',' ',title)
						if line.startswith('ISD'):
							issdate = re.search('ISD\s+(.*?)$',line).group(1)
							if issdate[6:] == "00":
								day = '01'
							else:
								day = issdate[6:]
							if issdate[4:6] == "00":
								month = '01'
							else:
								month = issdate[4:6]
							year = issdate[:4]
							issdate = year+'-'+month+'-'+day
							#print issdate
						if line.startswith("NCL"):
							numclaims = re.search('NCL\s+(.*?)$',line).group(1)
							
						 
						#Figure and sheet info
						if line.startswith('NDR'):
							numsheets = re.search('NDR\s+(.*?)$',line).group(1)
						if line.startswith('NFG'):
							numfigs = re.search('NFG\s+(.*?)$',line).group(1)
						
						#U.S. term of grant
						if line.startswith('TRM'):
							termpat = re.sub('[\n\t\r\f]+','',re.search('TRM\s+(.*?)$',line).group(1))
						if line.startswith('DCD'):
							disclaimerdate = re.sub('[\n\t\r\f]+','',re.search('DCD\s+(.*?)$',line).group(1))
							disclaimerdate = disclaimerdate[:4]+'-'+disclaimerdate[4:6]+'-'+disclaimerdate[6:]
						

						# Examiner info
						sequence = 0
						if line.startswith("EXA"):
							try:
								sequence +=1
								assistexam = re.search('EXA\s+(.*?)$',line).group(1).split("; ")
								assistexamfname = assistexam[1]
								assistexamlname = assistexam[0]
								examiner[id_generator()] = [patent_id,assistexamfname,assistexamlname,"assistant", "NULL"]
							except:
								pass
						if line.startswith("EXP"):
							try:
								sequence +=1
								primexam = re.search('EXP\s+(.*?)$',line).group(1).split("; ")
								primexamfname = primexam[1]
								primexamlname = primexam[0]
								examiner[id_generator()] = [patent_id,primexamfname,primexamlname,"primary", "NULL"]
							except:
								pass

						if line.startswith("ECL"):                     
							exemplary = re.search('ECL\s+(.*?)$',line).group(1)
							exemplary_list = exemplary.split(",")
					if numfigs!='' or numsheets!='':
					  	figureinfo[id_generator()] = [updnum,numfigs,numsheets]
					if termpat!='' or disclaimerdate!='':
						termofgrant[id_generator()] = [updnum,'',disclaimerdate,'',termpat,'']
					
					if int(appdate[:4]) >= 1992 and seriescode == "D":
						seriescode = "29"
						application[seriescode+'/'+appnum] = [patent_id,seriescode,seriescode+appnum,'US',appdate]
				except:
					print patent
			except:
				print patent

			patent_id = updnum

			

			
			#INVT - can be several
			try:
				inv_info = avail_fields['INVT'].split("\n\n\n\n\n")
				for n in range(len(inv_info)):
					fname = 'NULL'
					lname = 'NULL'
					invtcity = 'NULL'
					invtstate = 'NULL'
					invtcountry = 'NULL'
					invtzip = 'NULL'
					invtr47 = 'NULL'
					invtotherinfo = ''
					for enum,line in enumerate(inv_info[n].split("\n")):
						if line.startswith("NAM"):
							invtname = re.search('NAM\s+(.*?)$',line).group(1).split("; ")
							fname = invtname[1].replace("\r",'')
							lname = invtname[0].replace("\r",'')
							
						if line.startswith("CTY"):
							invtcity = re.search('CTY\s+(.*?)$',line).group(1)
							
						if line.startswith("STA"):
							invtstate = re.search('STA\s+(.*?)$',line).group(1)
						
						if line.startswith("CNT"):
							invtcountry = re.search('CNT\s+(.*?)$',line).group(1)
							if len(invtcountry) == 3 and invtcountry.endswith('X'):
								invtcountry = invtcountry[:-1]
								
						if line.startswith("ZIP"):
							invtzip = re.search('ZIP\s+(.*?)$',line).group(1)
						
						if line.startswith("R47"):
							invtr47 = re.search('R47\s+(.*?)$',line).group(1)
						
						if line.startswith('ITX'):
							invtotherinfo = []
							for nn in range(5):
								try:
									invtotherinfo.append(re.sub('ITX\s+','',inv_info[n].split("\n")[enum+nn]))
								except:
									break
							invtotherinfo = ' '.join([o for o in invtotherinfo if o!=''])
							invtotherinfo = re.sub('\s+',' ',invtotherinfo)
						
					
					loc_idd = id_generator()
					if invtcountry == "NULL":
						invtcountry = 'US'
					locs = [loc_idd,"NULL",invtcity,invtstate,invtcountry,invtzip]
					rawlocation[id_generator()] = [l.replace("\r",'') for l in locs]
					rawinventor[id_generator()] = [patent_id,"NULL",loc_idd,fname,lname,invtotherinfo,str(n),invtr47]
			except:
				pass
			
			#ASSG - can be several
			try:
				assg_info = avail_fields['ASSG'].split('\n\n\n\n\n')
				for n in range(len(assg_info)):    
					assgorg = ''
					assgfname = ''
					assglname = ''
					assgcity = 'NULL'
					assgstate = 'NULL'
					assgcountry = 'NULL'
					assgzip = 'NULL'
					assgtype = 'NULL'
					assgotherinfo = ''
					for enum,line in enumerate(assg_info[n].split("\n")):
						if line.startswith("NAM"):
							assgname = []
							for nn in range(5):
								if assg_info[n].split("\n")[enum+nn].startswith("CTY"):
									break
								else:
									assgname.append(re.sub('NAM\s+','',assg_info[n].split("\n")[enum+nn])) #= re.search('NAM\s+(.*?)$',line).group(1).split("; ")
							assgname = ' '.join(assgname).split('; ')
							if len(assgname) == 1:
								assgorg = re.sub('\s+',' ',assgname[0])
								assgfname = ''
								assglname = ''
							else:
								assgfname = assgname[1]
								assglname = assgname[0]
								assgorg = ''
						
						if line.startswith("CTY"):
							assgcity = re.search('CTY\s+(.*?)$',line).group(1)
							
						if line.startswith("STA"):
							assgstate = re.search('STA\s+(.*?)$',line).group(1)
						
						if line.startswith("CNT"):
							assgcountry = re.search('CNT\s+(.*?)$',line).group(1)
							if len(assgcountry) == 3 and assgcountry.endswith('X'):
								assgcountry = assgcountry[:-1]
						if line.startswith("ZIP"):
							assgzip = re.search('ZIP\s+(.*?)$',line).group(1)
						if line.startswith("COD"):
							assgtype = re.search("COD\s+(.*?)$",line).group(1)
						if line.startswith('ITX'):
							assgotherinfo = []
							for nn in range(5):
								try:
									assgotherinfo.append(re.sub('ITX\s+','',assg_info[n].split("\n")[enum+nn]))
								except:
									break
							assgotherinfo = ' '.join([o for o in assgotherinfo if o!=''])
							assgotherinfo = re.sub('\s+',' ',assgotherinfo)
							
					loc_idd = id_generator()
					if assgcountry == 'NULL':
						assgcountry = 'US'
					locs = [loc_idd,"NULL",assgcity,assgstate,assgcountry,assgzip]
					rawlocation[id_generator()] = [l.replace("\r",'') for l in locs]
					assgd = [patent_id,"NULL",loc_idd,assgtype,assgfname,assglname,assgorg,assgotherinfo,str(n)]
					rawassignee[id_generator()] = [a.replace("\r",'') for a in assgd]
			except:
				pass
			
			
			crossclass = 1
			#CLAS - should be several
			try:
				num = 0
				for line in avail_fields['CLAS'].split('\n'):
					if line.startswith('ICL'):
						intsec = 'NULL'
						mainclass = 'NULL'
						subclass = 'NULL'
						group = 'NULL'
						subgroup = 'NULL'
						intclass = re.search('ICL\s\s(.*?)$',line).group(1)
						if int(year) <= 1984:
							intsec = intclass[0]
							mainclass = intclass[1:3]
							if intsec == "D":
								subclass = intclass[3:5]
								group = "NULL"
								subgroup = "NULL"
							else:
								subclass = intclass[3]
								group = re.sub('^\s+','',intclass[4:7])
								subgroup = re.sub('^\s+','',intclass[7:])
						elif int(year) >= 1997 and updnum.startswith("D"):
							intsec = "D"
							try:
								mainclass = intclass[0:2]
								subclass = intclass[2:]
								group = 'NULL'
								subgroup = "NULL"
							except:
								mainclass = "NULL"
								subclass = "NULL"
								group = "NULL"
								subgroup = "NULL"
						else:
							intsec = intclass[0]
							mainclass = intclass[1:3]
							subclass = intclass[3]
							group = re.sub('^\s+','',intclass[4:7])
							subgroup = re.sub('^\s+','',intclass[7:])
						
						ipcr[id_generator()] = [patent_id,"NULL",intsec,mainclass,subclass, group,subgroup,"NULL","NULL","NULL","NULL","NULL","NULL",str(num)]
						num+=1     
						
					if line.startswith("OCL"):
						origmainclass = 'NULL'
						origsubclass = 'NULL'
						origclass = re.search('OCL\s\s(.*?)$',line).group(1).upper()
						origmainclass = re.sub('\s+','',origclass[0:3])
						origsubclass = re.sub('\s+','',origclass[3:])
						if len(origsubclass) > 3 and re.search('^[A-Z]',origsubclass[3:]) is None:
							origsubclass = origsubclass[:3]+'.'+origsubclass[3:]
						origsubclass = re.sub('^0+','',origsubclass)
						if re.search('[A-Z]{3}',origsubclass[:3]):
							origsubclass = origsubclass.replace('.','')
						if origsubclass != "":
							mainclassdata[origmainclass] = [origmainclass]
							uspc[id_generator()] = [patent_id,origmainclass,origmainclass+'/'+origsubclass,'0']
							subclassdata[origmainclass+'/'+origsubclass] = [origmainclass+'/'+origsubclass]
						
					if line.startswith("XCL"):
						crossrefmain = "NULL"
						crossrefsub = "NULL"
						crossrefclass = re.search('XCL\s\s(.*?)$',line).group(1).upper()
						crossrefmain = re.sub('\s+','',crossrefclass[:3])
						crossrefsub = re.sub('\s+','',crossrefclass[3:])
						if len(crossrefsub) > 3 and re.search('^[A-Z]',crossrefsub[3:]) is None:
							crossrefsub = crossrefsub[:3]+'.'+crossrefsub[3:]
						crossrefsub = re.sub('^0+','',crossrefsub)
						if re.search('[A-Z]{3}',crossrefsub[:3]):
							crossrefsub = crossrefsub.replace(".","")
						if crossrefsub != "":
							mainclassdata[crossrefmain] = [crossrefmain]
							uspc[id_generator()] = [patent_id,crossrefmain,crossrefmain+'/'+crossrefsub,str(crossclass)]
							subclassdata[crossrefmain+'/'+crossrefsub] = [crossrefmain+'/'+crossrefsub]
							crossclass+=1
						
			except:
				pass
						   
			# U.S. Patent Reference - can be several
			try:
				uspatref = avail_fields['UREF'].split("\n\n\n\n\n")
				for n in range(len(uspatref)):
					refpatnum = 'NULL'
					refpatname = 'NULL'
					refpatdate = 'NULL'
					refpatclass = 'NULL'
					for line in uspatref[n].split("\n"):
						if line.startswith('PNO'):
							refpatnum = re.search('PNO\s+(.*?)$',line).group(1)
						
						if line.startswith('ISD'):
							refpatdate = re.search('ISD\s+(.*?)$',line).group(1)
							if refpatdate[6:] == "00":
								day = '01'
							else:
								day = refpatdate[6:]
							if refpatdate[4:6] == "00":
								month = '01'
							else:
								month = refpatdate[4:6]
							year = refpatdate[:4]
							refpatdate = year+'-'+month+'-'+day
							
						if line.startswith('NAM'):
							refpatname = re.search('NAM\s+(.*?)$',line).group(1)
							
						if line.startswith('OCL'):
							refpatclass = re.search('OCL\s\s(.*?)$',line).group(1) 
					uspatentcitation[id_generator()] = [patent_id,refpatnum,refpatdate,refpatname,"NULL",'US',"NULL",str(n)]
			except:
				pass
			
			#Foreign reference - can be several
			try:
				foreignref = avail_fields['FREF'].split('\n\n\n\n\n')
				for n in range(len(foreignref)):
					forrefpatnum = 'NULL'
					forrefpatdate = 'NULL'
					forrefpatcountry = 'NULL'
					forrefpatclass = 'NULL'
					for line in foreignref[n].split("\n"):
						if line.startswith('PNO'):
							forrefpatnum = re.search('PNO\s+(.*?)$',line).group(1)
						
						if line.startswith('ISD'):
							forrefpatdate = re.search('ISD\s+(.*?)$',line).group(1)
							if forrefpatdate[6:] == "00":
								day = '01'
							else:
								day = forrefpatdate[6:]
							if forrefpatdate[4:6] == "00":
								month = '01'
							else:
								month = forrefpatdate[4:6]
							year = forrefpatdate[:4]
							forrefpatdate = year+'-'+month+'-'+day
							
						if line.startswith('CNT'):
							forrefpatcountry = re.search('CNT\s+(.*?)$',line).group(1)
							if len(forrefpatcountry) == 3 and forrefpatcountry.endswith('X'):
								forrefpatcountry = forrefpatcountry[:-1]
							
						if line.startswith('ICL'):
							forrefpatclass = re.search('ICL\s\s(.*?)$',line).group(1) 
					foreigncitation[id_generator()] = [patent_id,forrefpatdate,forrefpatnum,forrefpatcountry,"NULL",str(n)] 
			except:
				pass
			
			
			#Priority information = can be several prior apps
			try:
				priority = avail_fields['PRIR'].split('\n\n\n\n\n')
				for n in range(len(priority)):
					priorcountry = 'NULL'
					priorappdate = 'NULL'
					priorappnum = 'NULL'
					for line in priority[n].split("\n"):
						if line.startswith('CNT'):
							priorcountry = re.search('CNT\s+(.*?)$',line).group(1)
						
						if line.startswith('APD'):
							priorappdate = re.search('APD\s+(.*?)$',line).group(1)
							
						if line.startswith('APN'):
							priorappnum = re.search('APN\s+(.*?)$',line).group(1)
							
					prioritydata[id_generator()] = [patent_id,str(n),"",priorappnum,priorappdate,priorcountry] 
					
			except:
				pass
			
			
			#Other reference - can be several
			try:
				otherreflist = avail_fields['OREF'].split('\n\n\n\n\n')
				for n in range(len(otherreflist)):
					if re.search('PAL ',otherreflist[n]):
						allrefs = otherreflist[n].split('PAL ')
						del allrefs[0]
						for a in range(len(allrefs)):
							otherref = 'NULL'
							otherref = re.sub('\s+',' ',allrefs[a])
							otherref = re.sub('^\s+[A-Z]+\s+','',otherref)
							#print otherref
							otherreference[id_generator()] = [patent_id,otherref,str(a)]
					else:
						otherref = 'NULL'
						otherref = re.sub('\s+',' ',otherreflist[n])
						otherref = re.sub('^\s+[A-Z]+\s+','',otherref)
						#print otherref
						otherreference[id_generator()] = [patent_id,otherref,str(n)]
			except:
				pass
			
			
			#PCT info
			try:
				pctinfo = avail_fields['PCTA'].split('\n\n\n\n\n')
				#print pctinfo
				for n in range(len(pctinfo)):
					pctnum = 'NULL'
					pct371 = 'NULL'
					pct102 = 'NULL'
					pctdate = 'NULL'
					pctpubnum = 'NULL'
					pctpubdate = 'NULL'
					for line in pctinfo[n].split("\n"):
						if line.startswith('PCN'):
							pctnum = re.sub('[\n\t\r\f]+','',re.search('PCN\s+(.*?)$',line).group(1))
						if line.startswith('PD1'):
							pct371 = re.sub('[\n\t\r\f]+','',re.search('PD1\s+(.*?)$',line).group(1))
						if line.startswith('PD2'):
							pct102 = re.sub('[\n\t\r\f]+','',re.search('PD2\s+(.*?)$',line).group(1))
						if line.startswith('PD3'):
							pctdate = re.sub('[\n\t\r\f]+','',re.search('PD3\s+(.*?)$',line).group(1))
						if line.startswith('PCP'):
							pctpubnum = re.sub('[\n\t\r\f]+','',re.search('PCP\s+(.*?)$',line).group(1))
						if line.startswith('PCD'):
							pctpubdate = re.sub('[\n\t\r\f]+','',re.search('PCD\s+(.*?)$',line).group(1))
					
					pctdata[id_generator()] = [patent_id,pctpubnum,pctpubdate,pct371,"WO","A","wo_grant",pct102]
					pctdata[id_generator()] = [patent_id,pctnum,pctdate,pct371,"WO","00","pct_application",pct102]
					#pct_data.writerow(['uuid', 'patent_id', 'rel_id', 'date', '371_date', 'country', 'kind', "doc_type","sequence"])
			except:
				pass
			
			#Legal information - can be several
			try:
				legal_info = avail_fields['LREP'].split("\n\n\n\n\n")
				for n in range(len(legal_info)):
					nnnn = 0
					lawyer_info = legal_info[n].split('\n')
					del lawyer_info[0]
					del lawyer_info[-1]
					for line in lawyer_info:
						legalcountry = 'NULL'
						legalfirm = 'NULL'
						attfname = 'NULL'
						attlname = 'NULL'
						if line.startswith("FRM"):
							legalfirm = re.search('FRM\s+(.*?)$',line).group(1)
							#print legalfirm
						if line.startswith("FR2"):
							attorney = re.search('FR2\s+(.*?)$',line).group(1).split('; ')
							attfname = attorney[1]
							attlname = attorney[0]
							#print attlname
						if line.startswith('CNT'):
							legalcountry = re.search('CNT\s+(.*?)$',line).group('; ')
							if len(legalcountry) == 3 and legalcountry.endswith('X'):
								legalcountry = legalcountry[:-1]
									
					
						if [attfname,attlname,legalfirm,legalcountry] != ['NULL','NULL','NULL','NULL']:
							rawlawyer[id_generator()] = ["NULL",patent_id,attfname,attlname,legalfirm,legalcountry,str(nnnn)]
							nnnn+=1
						
			except:
				pass
			
			# Abstract
			abst = 'NULL'
			try:
				abst = re.sub('PAL\s+','',avail_fields['ABST'])
				abst = re.sub('PAR\s+','',abst)
				abst = re.sub('TBL\s+','',abst)
				abst = re.sub('\s+',' ',abst)
				
			except:
				pass
			
			# Brief summary
			bsum = 'NULL'
			try:
				bsum = re.sub('PAC\s+.*?\n','',avail_fields['BSUM'])
				bsum = re.sub('PAR\s+',' ',bsum)
				bsum = re.sub('PA\d+\s+',' ',bsum)
				bsum = re.sub('TBL\s+',' ',bsum)
				bsum = re.sub('\s+',' ',bsum)
				bsum = re.sub('^\s','',bsum)
			except:
				pass
			
			# Drawing description
			drawdesc = 'NULL'
			try:
				drawdesc = re.sub('\s+',' ',avail_fields['DRWD'])
				drawdesc = re.sub('\s$','',drawdesc)
				drawdesc = re.split(r'( PAR | PAL | PAC )', drawdesc)
				drawdesc = filter(None, drawdesc) #this doesn't work combined with next filter, so separate for now
				drawdesc = filter(lambda x : x not in [' PAR ', ' PAL ',' PAC ', 'BRIEF DESCRIPTION OF THE DRAWINGS'," BRIEF DESCRIPTION OF THE DRAWING", "DESCRIPTION OF THE DRAWING"], drawdesc)
				draw_sequence = 1
				for n in range(0,len(drawdesc) ):
					if drawdesc[n] >1: #filter out blank entries
						drawdescdata[id_generator()] =[patent_id,drawdesc[n],str(draw_sequence)] 
						draw_sequence +=1
			except:
				pass

			# Detail description
			detdesc = None
			try:
				detdesc = re.sub('PAR\s+',' ',avail_fields['DETD'])
				detdesc = re.sub('PAC\s+',' ',detdesc)
				detdesc = re.sub('PA\d+\s+',' ',detdesc)
				detdesc = re.sub('TBL\s+','',detdesc)
				detdesc = re.sub('\s+',' ',detdesc)
				detail_desc_text[id_generator()] = [patent_id, detdesc, len(detdesc)]
			except:
				pass


			# Parent case for US rel docs
			try:
				#parentcase = avail_fields['PARN'].split('\n')
				parentcase = avail_fields['PARN'].split("PAC ")
				parentcase = [p for p in parentcase if len(re.sub('\s+','',p))!=0]
				if len(parentcase)>1:
					parent = 'PAC '+parentcase[0]
					nump = 1
					parent = re.sub('PAC\s+[A-Z-\s+]+','',parent)
					if parent == '':
						nump = 2
						parent = 'PAC '+parentcase[1]
						parent = re.sub('PAC\s+[A-Z-\s+]+','',parent)
					parent = re.sub('PAR\s+','',parent)
					parent = re.sub('\.Iaddend\.|\.Iadd\.','',parent)
					parent = re.sub('^h','Th',parent)
					parent = re.sub('^r','Cr',parent)
					parent = re.sub('\s+',' ',parent)
					parent = re.sub('"','',parent)
				
					if bsum=='NULL':
						try:
							bsum = re.sub('PAC\s+.*?\n','',"PAC "+'PAC '.join(parentcase[nump:]))
							bsum = re.sub('PAR\s+',' ',bsum)
							bsum = re.sub('PA\d+\s+',' ',bsum)
							bsum = re.sub('TBL\s+',' ',bsum)
							bsum = re.sub('\s+',' ',bsum)
							bsum = re.sub('^\s','',bsum)
						except:
							pass
					else:
						pass
				else:
					parent = 'PAC '+parentcase[0]
					parent = re.sub('PAC\s+[A-Z-\s+]+','',parent)
					parent = re.sub('\.Iaddend\.|\.Iadd\.','',parent)
					parent = re.sub('^h','Th',parent)
					parent = re.sub('\s+',' ',parent)
					parent = re.sub('"','',parent)
					if bsum=='NULL':
						parent = parent.split("PAR ")
						bsum = re.sub('PAR\s+','','PAR '+'PAR '.join(parent[1:]))
						bsum = re.sub('PA\d+\s+',' ',bsum)
						bsum = re.sub('TBL\s+',' ',bsum)
						parent = parent[0]
				
				# this is experimental for 1976-2001; need to address further later to extract individual app and patent numbers
				reldoc = ''
				for doctype in reldoctype:
					doctype=doctype.replace('_',' ')
					if re.search(doctype,parent.lower().replace('conti9nuation','continuation')):
						reldoc = doctype
						data = re.findall(doctype+' of.*?\s([\d+,]?\d+,\d+)\W|'+doctype+' of.*?\s(\d+/\d+,\d+)\W',parent.lower().replace('conti9nuation','continuation'))
						for l in range(len(data)):
							usreldoc[id_generator()] =[updnum,doctype.replace(" ","_").replace("-",'_'),'',data[l].replace(',',''),'','','',str(l),'']
				
				relappdata[id_generator()] = [updnum,parent]
				
					
			except:
				pass
			patentdata[patent_id] = [type_kind[apptype][1],updnum,'US',issdate,abst,title,type_kind[apptype][0],numclaims,d]
			
			# Claims data parser
			datum = {}
			check = re.search('\nCLMS',i)
			if check:
				try:
					claims = re.search('CLMS(.*)\nDCLM',i,re.DOTALL).group(1)
				except:    
					try:
						claims = re.search('CLMS(.*)\nEOV',i,re.DOTALL).group(1)
					except:
						try:
							claims = re.search('CLMS(.*)\nEOF',i,re.DOTALL).group(1)
						except:
							try:
								claims = re.search('CLMS(.*)',i,re.DOTALL).group(1)
							except:
								claims = ''
				clnums = claims.split('NUM  ')
				del clnums[0]
				nnn = []
				if len(clnums) > 1:
					for c in range(len(clnums)):
						needed = re.search('(\d+)\.\s(.*)',clnums[c],re.DOTALL)
						try:
							number = needed.group(1)
							text = needed.group(2)
							text = re.sub('PA.\s+|TB.\s+|EQ.\s+','',text)
							text = re.sub('[\n\t\r\f]+','',text)
							text = re.sub('\s+',' ',text)
							text = re.sub('_+','',text)
							datum[int(number)] = text
							nnn.append(int(number))
						except:
							text = re.sub('PA.\s+|TB.\s+|EQ.\s+','',clnums[c])
							text = re.sub('[\n\t\r\f]+','',text)
							text = re.sub('\s+',' ',text)
							text = re.sub('_+','',text)
							try:
								datum[nnn[-1]]+=text
							except:
								pass
				else:
					try:
						needed = re.findall('\d+\.',claims)
						for ne in range(len(needed)-1):
							number = needed[ne]
							text = re.search(needed[ne]+'(.*)'+needed[ne+1],claims,re.DOTALL).group(1)
							text = re.sub('PA.\s+|TB.\s+|EQ.\s+','',text)
							text = re.sub('[\n\t\r\f]+','',text)
							text = re.sub('\s+',' ',text)
							text = re.sub('_+','',text)
							text = re.sub('^\s+','',text)
							number = re.sub('\.','',number)
							datum[int(number)] = text
							#print text
						number = needed[-1]
						text = re.search(number+'(.*)',claims,re.DOTALL).group(1)
						text = re.sub('PA.\s+|TB.\s+|EQ.\s+','',text)
						text = re.sub('[\n\t\r\f]+','',text)
						text = re.sub('\s+',' ',text)
						text = re.sub('_+','',text)
						number = re.sub('\.','',number)
						text = re.sub('^\s+','',text)
						datum[int(number)] = text
					except:
						try:
							number = '1'
							text = re.search('STM  (.*)',claims,re.DOTALL).group(1)
							text = re.sub('PA.\s+|TB.\s+|EQ.\s+','',text)
							text = re.sub('[\n\t\r\f]+','',text)
							text = re.sub('\s+',' ',text)
							text = re.sub('_+','',text)
							number = re.sub('\.','',number)
							text = re.sub('^\s+','',text)
							datum[int(number)] = text
						except:
							pass
				
				datum = sorted(datum.items())
				for k,v in datum:
					exemplary = False
					if str(k) in exemplary_list:
						exemplary = True
					claim_stripped = re.sub('^\s\d+\.\s','',v)
					claim_stripped = claim_stripped.lstrip('12234567890.')
					claimsdata[id_generator()] = [updnum,claim_stripped,"NULL",str(k), exemplary]
					#claimsdata[id_generator()] = [updnum,re.sub('^\s\d+\.\s','',v),"NULL",str(k), exemplary]
				if len(datum) == 0:
					pass
			else:
				check = re.search('\nDCLM',i)
				if check:
					try:
						claims = re.search('DCLM(.*)\nEOV',i,re.DOTALL).group(1)
					except:    
						try:
							claims = re.search('DCLM(.*)\nEOF',i,re.DOTALL).group(1)
						except:
							try:
								claims = re.search('DCLM(.*)',i,re.DOTALL).group(1)
							except:
								claims = ''
						
					try:
						text = re.sub('[\n\t\r\f]+','',claims)
						text = re.sub('^\W+[A-Z]+\s+','',text)
						text = re.sub('\s+',' ',text)
					except:
						pass
					exemplary = False
					if "1" in exemplary_list:
						exemplary = True
					text_stripped = re.sub('^PAR\s+','',text)
					text_stripped = text_stripped.lstrip('12234567890.')
					claimsdata[id_generator()]=[updnum,text_stripped,"NULL",'1', exemplary]
					#claimsdata[id_generator()]=[updnum,re.sub('^PAR\s+','',text),"NULL",'1', exemplary]
				else:
					pass

		
			draw_desc_textfile = csv.writer(open(os.path.join(fd2,'draw_desc_text.csv'),'ab'),delimiter='\t')
			for k,v in drawdescdata.items():
				draw_desc_textfile.writerow([k]+v)
			
			
			brf_sum_textfile = csv.writer(open(os.path.join(fd2,'brf_sum_text.csv'),'ab'),delimiter='\t')
			brf_sum_textfile.writerow([id_generator(),patent_id, bsum])
			
			det_desc_textfile = csv.writer(open(os.path.join(fd2,'detail_desc_text.csv'),'ab'),delimiter='\t')
			for k,v in detail_desc_text.items():
				det_desc_textfile.writerow([k] + v)
			#det_desc_textfile.writerow([id_generator(),patent_id,detdesc, len(detdesc)])
			
			patfile = csv.writer(open(os.path.join(fd2,'patent.csv'),'ab'),delimiter='\t')
			for k,v in patentdata.items():
				patfile.writerow([k]+v)
			"""
			### Need to work more on parsing from raw data
			usreldocfile = csv.writer(open(os.path.join(fd2,'usreldoc.csv'),'ab'),delimiter='\t')
			for k,v in usreldoc.items():
				usreldocfile.writerow([k]+v)
			"""
			rel_app_textfile = csv.writer(open(os.path.join(fd2,'rel_app_text.csv'),'ab'),delimiter='\t')
			for k,v in relappdata.items():
				rel_app_textfile.writerow([k]+v)
	
			pct_datafile = csv.writer(open(os.path.join(fd2,'pct_data.csv'),'ab'),delimiter='\t')
			for k,v in pctdata.items():
				pct_datafile.writerow([k]+v)
			
			examfile = csv.writer(open(os.path.join(fd2,'examiner.csv'),'ab'),delimiter='\t')
			for k,v in examiner.items():
				examfile.writerow([k]+v)

			claimsfile = csv.writer(open(os.path.join(fd2,'claim.csv'),'ab'),delimiter='\t')
			for k,v in claimsdata.items():
				claimsfile.writerow([k]+v)            
			
			appfile = csv.writer(open(os.path.join(fd2,'application.csv'),'ab'),delimiter='\t')
			for k,v in application.items():
				appfile.writerow([k]+v)
			
			rawinvfile = csv.writer(open(os.path.join(fd2,'rawinventor.csv'),'ab'),delimiter='\t')
			for k,v in rawinventor.items():
				rawinvfile.writerow([k]+v)
			
			rawassgfile = csv.writer(open(os.path.join(fd2,'rawassignee.csv'),'ab'),delimiter='\t')
			for k,v in rawassignee.items():
				rawassgfile.writerow([k]+v)
			
			ipcrfile = csv.writer(open(os.path.join(fd2,'ipcr.csv'),'ab'),delimiter='\t')
			for k,v in ipcr.items():
				ipcrfile.writerow([k]+v)
			
			uspcfile = csv.writer(open(os.path.join(fd2,'uspc.csv'),'ab'),delimiter='\t')
			for k,v in uspc.items():
				uspcfile.writerow([k]+v)
			
			uspatentcitfile = csv.writer(open(os.path.join(fd2,'uspatentcitation.csv'),'ab'),delimiter='\t')
			for k,v in uspatentcitation.items():
				uspatentcitfile.writerow([k]+v)

			foreigncitfile = csv.writer(open(os.path.join(fd2,'foreigncitation.csv'),'ab'),delimiter='\t')
			for k,v in foreigncitation.items():
				foreigncitfile.writerow([k]+v)

			otherreffile = csv.writer(open(os.path.join(fd2,'otherreference.csv'),'ab'),delimiter='\t')
			for k,v in otherreference.items():
				otherreffile.writerow([k]+v)
			
			forpriorityfile = csv.writer(open(os.path.join(fd2,'foreign_priority.csv'),'ab'),delimiter='\t')
			for k,v in prioritydata.items():
				forpriorityfile.writerow([k]+v)
			
			rawlawyerfile = csv.writer(open(os.path.join(fd2,'rawlawyer.csv'),'ab'),delimiter='\t')
			for k,v in rawlawyer.items():
				rawlawyerfile.writerow([k]+v)
			
			figurefile = csv.writer(open(os.path.join(fd2,'figures.csv'),'ab'),delimiter='\t')
			for k,v in figureinfo.items():
				figurefile.writerow([k]+v)
			
			us_term_of_grantfile = csv.writer(open(os.path.join(fd2,'us_term_of_grant.csv'),'ab'),delimiter='\t')
			for k,v in termofgrant.items():
				us_term_of_grantfile.writerow([k]+v)
			
		# except:
		# 	traceback.print_exc()
			  
	rawlocfile = csv.writer(open(os.path.join(fd2,'rawlocation.csv'),'ab'),delimiter='\t')
	for k,v in rawlocation.items():
		rawlocfile.writerow(v)
			
	mainclassfile = csv.writer(open(os.path.join(fd2,'mainclass.csv'),'ab'),delimiter='\t')
	for k,v in mainclassdata.items():
		mainclassfile.writerow(v)
	
	subclassfile = csv.writer(open(os.path.join(fd2,'subclass.csv'),'ab'),delimiter='\t')
	for k,v in subclassdata.items():
		subclassfile.writerow(v)
		
parse_patents("D:/PV_Patches/MissingApplicationDate/DataIn", "D:/PV_Patches/MissingApplicationDate/DataOut")