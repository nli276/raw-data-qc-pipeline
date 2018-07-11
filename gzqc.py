#!/nli/anaconda2/bin/python
'''
This script does basic sanity check for raw compressed csv files.
'''

import os
import gzip
import io
import subprocess

import ConfigParser
import sys

import pandas as pd

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

######show results of input#
config = ConfigParser.ConfigParser()
config.read(sys.argv[1])

rawdata = gzqc(datapath=config.get('input','datapath'),
               outputpath=config.get('output','outputpath'),
               datatype=config.get('input','datatype'),
               excludefiles=config.get('input','excludefiles').split('\n'),
               delim=config.get('input','delim')
               )

print('---Count files---')
rawdata.countfiles()
print('---Format check---')
rawdata.formatcheck()
print('---File shape---')
rawdata.csvshape()
print('---Generate report---')
rawdata.exportreport()
