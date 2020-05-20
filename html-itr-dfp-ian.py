# -*- coding: iso-8859-15 -*-

import os
import urllib
import zipfile

os.chdir('C:\\Users\\oikawa\\Documents\\Ações\\projeto SQL\\teste\\')

code = {'ITR':'1', 'DFP':'2', 'IAN':'3'} #fixo
ext = {'ITR':'.WTL', 'DFP':'.WFL', 'IAN':'.WAN'} #fixo

anos = range(2005, 2008)
periodo = {'ITR':['%s/%s/%s' %(d, m, y) for m in ('03','06','09') for y in anos for d in (('30',),('31',))[m == '03']],\
           'DFP': ['31/12/%s' % y for y in anos],\
           'IAN':['31/12/%s' % y for y in anos]}

razao = 'SERGEN SERVICOS GERAIS DE ENG S.A.'
razao = razao.replace(' ','%20')
pregao = 'SERGEN'
ccvm = '10596'
tipo = 'ITR'

for data in periodo[tipo]:
    link = 'http://siteempresas.bovespa.com.br/dxw/download.asp?moeda=L&tipo=%s&data=%s&razao=%s&site=C&pregao=%s&ccvm=%s' % (code[tipo], data, razao, pregao, ccvm)
    arq = pregao + '-' + data.replace('/','-') + ext[tipo]

    urllib.urlretrieve(link, 'temp.zip')
    print arq

    fromZip = zipfile.ZipFile('temp.zip')
    toZip = zipfile.ZipFile(arq, 'w')
    for cada in fromZip.namelist():
        head, tail = os.path.split(cada)
        tempFile = open(tail, 'wb')
        tempFile.write(fromZip.read(cada))
        tempFile.close()
        toZip.write(tail)
    toZip.close()
        
