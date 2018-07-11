#!/nli/anaconda2/bin/python
'''
This script does more sanity check for csv files (compressed or not) using pandas (chunck), to make sure 
1) the whole csv can be read with no issues;
2) the number of rows and columns are as expected comparing to client's data dictionary
'''

import os
import gzip
import io
import subprocess

import ConfigParser
import sys

import pandas as pd
import fnmatch
from collections import OrderedDict

def gzreader(fname=None):
    gz = gzip.open(fname, 'rb')
    f = io.BufferedReader(gz)
    for lines in f:
        yield lines
    gz.close()

class gzqc:
    def __init__(self, datapath=None, outputpath=None, datatype=None, excludefiles=None, delim=None):
        self.datapath = datapath
        self.outputpath = outputpath
        self.datatype = datatype
        self.excludefiles = excludefiles
        self.delim = delim
        os.chdir(self.datapath)
        
    
    def countfiles(self):
        self.allfiles = os.listdir(self.datapath)
        #self.files = list(set(self.allfiles).difference(set(excludefiles)))
        self.files = []
        for names in self.allfiles:
            if names.endswith(self.datatype) and (names not in self.excludefiles):
                self.files.append(names)
        self.filecounts = len(self.files)
        print '%s%s%s%s%s%s'%('There are ',self.filecounts,' ',self.datatype,' files in folder ',self.datapath)
        print self.files
        return self.filecounts
        
    def formatcheck(self):
        noheaderfiles=[]
        brokenlinefiles=[]
        for names in self.files:
            l=[]
	    nq=[]
            ncol=[]
            for idx, row in enumerate(gzreader(names)):
                l.append(len(row))
                ncol.append(row.count(self.delim)+1)
		nq.append(row.count('"'))
                if idx > 9: break
            
            if nq[0]!=0:
		noheaderfiles.append(names)
                print '%s%s'%(names,' has no header.')
                #print l
            
            if min(l)<2 or ((max(ncol)!=min(ncol)) and (max(nq) != 2*min(ncol))):#empty line (extra \n) detection, and different ncol count
                brokenlinefiles.append(names)
                print '%s%s'%(names,' has broken lines.')
                #print '%s%s'%('minimum line length: ',min(l))
                print '%s%s'%('minimum number of columns: ',min(ncol))
                print '%s%s'%('maximum number of columns: ',max(ncol))
                
        self.badfiles = noheaderfiles + list(set(brokenlinefiles) - set(noheaderfiles))
        print '%s%s'%(len(noheaderfiles),' files have no headers:')
        print noheaderfiles
        print '%s%s'%(len(brokenlinefiles),' files have broken lines:')
        print brokenlinefiles
        self.goodfiles = list(set(self.files) - set(self.badfiles))
        return self.badfiles
    
    def csvshape(self):
        self.shapes=[]
        self.report=[]

        for names in self.files:
            if names not in self.badfiles:
                #nrow
                args1 = ['zcat', names]
                args2 = ['wc', '-l']
                p1 = subprocess.Popen(args1, stdout=subprocess.PIPE)
                p2 = subprocess.Popen(args2, stdin=p1.stdout, stdout=subprocess.PIPE)
                # Allow p1 to receive a SIGPIPE if p2 exits.
                p1.stdout.close()
                print names + ' shape: '
                nrow = int(p2.communicate()[0])-1
                print '%s%s'%(nrow,' rows')
                
                                
                #ncol
                ncol=[]
                for idx, row in enumerate(gzreader(names)):
                    ncol.append(row.count(self.delim)+1)
                    if idx > 5: break
                if max(ncol)==min(ncol):
                    print '%s%s'%(max(ncol),' columns')
                else: 
                    print '%s%s'%('uneven number of delimiter found in ',names)
                    
                self.shapes.append((nrow,min(ncol)))
                self.report.append("|".join([names,str(nrow),str(min(ncol))]))
        return self.report
	print self.report
    
    def exportreport(self):
        df = pd.DataFrame(self.report)
        df.columns=['file|nrow|ncol']
        self.outputfile=os.path.join(self.outputpath,'report.txt')
        df.to_csv(self.outputfile,index=False)
        print '%s%s'%('Report generated: ', self.outputfile)
        
        
if __name__ == '__main__':
    print 'This script is being run by itself.'
else:
    print 'This script is imported from another module.'

###### pandas validation #######
class pdvaliddim(gzqc):
    def __init__(self, datapath, outputpath, datatype, excludefiles, delim, dictxls):
        gzqc.__init__(self, datapath, outputpath, datatype, excludefiles, delim)
        self.dictxls=dictxls
        
    def readexcel(self):
        print "Reading data dictionary: " + self.dictxls
        self.dictionary = pd.read_excel(self.dictxls,sheet_name='DIMENSIONS')
        self.dictkeys = self.dictionary['Table Name'].unique().tolist()
        
    def readdata(self):
        chunksize = 10000
        dkeylist=[]
        fnamelist=[]
        ncolelist=[]#expected ncol from dictionary
        nrowlist=[]
        ncollist=[]
        errorlist=[]
        missingcolumnslist=[]
        extracolumnslist=[]
        
        for dkey in self.dictkeys:
        #for dkey in ['DIMENSIONS']:#for test
            print '<--key-->' + dkey
            
            for fname in self.files:
                #I thought fnmatch is not case sensitive...
                if fnmatch.fnmatch(fname.lower(), ('*_'+dkey.lower()+'*')):
                    print "%s matches pattern %s " % (fname,dkey)
                    dkeylist.append(dkey)
                    fnamelist.append(fname)
                    nrow=0
                    ncol=0
                    ncole=len(self.dictionary[self.dictionary['Table Name']==dkey]['Column Physical Name'].tolist())
                    #ncole=3#for test
                    ncolelist.append(ncole)
                    cerror=None
                    try: 
                        for chunk in pd.read_csv(fname,
                                                 chunksize=chunksize,
                                                 compression='gzip',
                                                 nrows=100000#for testing
                                                 #error_bad_lines=False
                                                ):
                            nrow+=chunk.shape[0]
                            ncol=chunk.shape[1]
                            #print chunk.shape#for testing
	   	    except Exception as e:
                        errortype = e.message.split('.')[0].strip()
                        if errortype=='Error tokenizing data':
                            cerror=e.message.split(':')[1].strip().replace(',','')
                            print '%s%s%s%s%s'%(nrow," rows ",ncol," columns ",cerror)
                            nrowlist.append(nrow)
                            ncollist.append(ncol)
                            errorlist.append(cerror)
                            missingcolumnslist.append(str(list(set(self.dictionary[self.dictionary['Table Name']==dkey]['Column Physical Name'].str.strip()) - set(chunk.columns.str.strip()))))
                            extracolumnslist.append(str(list(set(chunk.columns.str.strip()) - set(self.dictionary[self.dictionary['Table Name']==dkey]['Column Physical Name'].str.strip()))))
                        else:
                            cerror='Error: Unknown error' 
                            print '%s%s%s%s%s'%(nrow," rows ",ncol," columns ",cerror)
                            nrowlist.append(nrow)
                            ncollist.append(ncol)
                            errorlist.append(cerror)
                            missingcolumnslist.append(None)
                            extracolumnslist.append(None)
            	else:
			     if ncol!=ncole:
                            cerror="Error: Columns do not match data dictionary." 
                                
                        print '%s%s%s%s%s'%(nrow," rows ",ncol," columns ",cerror)
                        nrowlist.append(nrow)
                        ncollist.append(ncol)
                        errorlist.append(cerror)
                        missingcolumnslist.append(str(list(set(self.dictionary[self.dictionary['Table Name']==dkey]['Column Physical Name'].str.strip()) - set(chunk.columns.str.strip()))))
                        extracolumnslist.append(str(list(set(chunk.columns.str.strip()) - set(self.dictionary[self.dictionary['Table Name']==dkey]['Column Physical Name'].str.strip()))))


        s = pd.DataFrame.from_dict(OrderedDict([('name_pattern',dkeylist),('file_name',fnamelist),('ncol_dict',ncolelist),('ncol_pd',ncollist),('nrow_pd',nrowlist),('error',errorlist),('missing_col',missingcolumnslist),('extra_col',extracolumnslist)]), orient='index')
        s_t = s.transpose()
        s_t['sum_nrow_pd'] = s_t.groupby('name_pattern')['nrow_pd'].transform(sum)
        s_t['sum_nfile'] = s_t.groupby('name_pattern')['nrow_pd'].transform(lambda x: len(x))
        
        self.summary=s_t
        print "Name patterns %s have no matching files found among the good format files." %(list(set(self.dictkeys) - set(dkeylist)))
    def reportsummary(self):
        self.outputfile3=os.path.join(self.outputpath,'dim_report_summary.txt')
        self.summary.to_csv(self.outputfile3,index=False,sep='|')
        print '%s%s'%('Report summary generated: ', self.outputfile3)
        
    def __str__(self):
        return "input path: %s  output path: %s" % (self.datapath, self.outputpath)


######show results of input#
config = ConfigParser.ConfigParser()
config.read(sys.argv[1])

pdrawdatadim = pdvaliddim(datapath=config.get('input','datapath'),
               outputpath=config.get('output','outputpath'),
               datatype=config.get('input','datatype'),
               excludefiles=config.get('input','excludefiles').split('\n'),
               delim=config.get('input','delim'),
               dictxls=config.get('ddict','dictxls')
              )

print('---Count files---')
pdrawdatadim.countfiles()
print('---Format check---')
pdrawdatadim.formatcheck()
#print('---File shape---')
#pdrawdata.csvshape()
print('---Read data dictionary---')
pdrawdatadim.readexcel()
print('---Read CSV---')
pdrawdatadim.readdata()
print('---Generate report---')
pdrawdatadim.reportsummary()
