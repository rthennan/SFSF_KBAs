# coding: utf-8
# Moving functions to a different file and optimizing the same 
from multiprocessing import Process,Array,Value
import multiprocessing
import os
import math
import time
import requests
import bs4 as bs
import pandas as pd
import datetime

print(datetime.date.today(),'- Web scrapper','Started')
chunkSize = 8
inFile = 'KBA_list1.xlsx'
outFile = datetime.datetime.today().strftime('%d_%b_%Y')

url_pre = 'https://apps.support.sap.com/sap/support/knowledge/public/en/'
headers = {
  "User-Agent": "python-requests/2.21.0",
  "X-Requested-With": "XMLHttpRequest"
}

def find(kbas):
    for rowNum in range(len(kbas)):
        try:
            kbaNum = kbas.iat[rowNum,0]
            urFull = url_pre+str(kbaNum)
            r = requests.get(urFull, headers=headers)

            if (r.status_code == 200):
                soup = bs.BeautifulSoup(r.content, "html.parser")
                subText = str(soup.find('title').text.replace(u'\xa0', u'')) 
                #For Human Readable Excel File
                kbas.at[rowNum,'KBA'] = subText
                #For Markdown
                hyperlinkText = "["+subText+"]"
                kbas.at[rowNum,'markdwn'] = hyperlinkText+"("+url_pre+str(kbaNum)+")  " #for Markdown
                
                time.sleep(1)
            else:
                pass
        except:
            pass
    return kbas
                
           
def findAll(threadNumber,pivot,rowCount,cols):
    killThread = False
    global chunkSize
    global inFile
    
    while True :
        
        pivotLocal = pivot.value

        if pivotLocal >= rowCount.value:
            killThread = True
        else:
            pivot.value = pivotLocal + chunkSize
        
        if killThread == True:
            break
        
        try:

            kba = pd.read_excel(inFile,header=0,skiprows=pivotLocal, nrows=chunkSize,names=cols)
            kba['KBA'] = None
            kba['markdwn'] = None
            kba = find(kba)
            
 
        finally:
            kba.to_excel(str(pivotLocal)+".xls",index=False)
            print("ProcessNumber = %d and Pivot = %d " % (threadNumber,pivotLocal))

def main():
    print(datetime.date.today(),'- Web scrapper','Started')
    global inFile
    global outFile
    start = time.time()
    threadLimit = multiprocessing.cpu_count()   #Number of Threads to be used
    df = pd.read_excel(inFile)
    print(datetime.date.today(),'- Input read completed')
    cols = df.columns
    rowCount =  Value('i', 0)
    rowCount.value = df.shape[0]
    #df.shape[0]
    pivot = Value('i', 0)
    cols = Array('i', range(10))
    cols = df.columns
    #Start Processes
    del df
    threads = []
 
    for i in range(threadLimit):
        t = Process(target=findAll, args=(i,pivot,rowCount,cols))
        threads.append(t)

    # Start all threads
    for x in threads:
        x.start()

    # Wait for all of them to finish
    for x in threads:
        x.join()

    list_ = []
    global chunkSize
    rangeVal = math.ceil(rowCount.value/chunkSize)
    print(rangeVal)
    for index in range(rangeVal):
        chunkFile = str(index * chunkSize)+".xls"
        df = pd.read_excel(chunkFile,index_col=None, header=0)
        list_.append(df)
        os.remove(chunkFile)
    
    combined_xls = pd.concat(list_, axis = 0, ignore_index = True)
    combined_xls.dropna(inplace=True)
    mrkdown = combined_xls[['markdwn']]
    humanKBA = combined_xls[['KBA']]  
    #kbaHuman 
    humanKBA.to_excel( outFile+'.xlsx', index=False )
 
    #Exporting the list to markdown for direct embedding in a GitHub page
    mrkdown.to_csv( outFile+'.txt', index=False, header=False)
    
    
    end = time.time()
    print(end - start)
    print(datetime.date.today(),'- Webscrapper Finshed')
if __name__ == '__main__':
    __spec__ = None
    main()