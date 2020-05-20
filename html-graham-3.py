# -*- coding: iso-8859-15 -*-
from __future__ import with_statement
from HTMLParser import HTMLParser
import urllib2
import datetime

class MyHTMLParser(HTMLParser):
    def reset(self):
        HTMLParser.reset(self)
        self.sig = False
        self.sig2 = True
        self.cotacao = []

#    def handle_comment(self, data):
#        if data == 'P/VP':
#            print data
#            self.sig = True

    def handle_starttag(self, tag, attrs):
        if tag == 'td':
            self.sig = True
        if tag == 'div' and attrs[0][1] == 'rodape':
            self.sig2 = False
        

#    def handle_endtag(self, tag):
#        if tag == 'table' and self.sig: self.sig = False

    def handle_data(self, data):
        if self.sig and self.sig2:
            temp = str(data).strip()
            if temp != '':
                self.cotacao.append(temp)
#        if self.sig:
#            if self.sig2:
#                self.cotacao.append(str(data).strip())
#            if data == 'Taxa de Venda':
#                self.sig2 = True

g = open("graham-" + str(datetime.date.today()) + ".txt", 'w')
g.write('papel cotacao graham P/L P/VP P/Cap.Giro P/Ativ.Circ.Liq Div.Yield ROIC ROE Cres.Rec(5a) Div.Br/Patrim\n')
with open("t.txt") as f:
    for line in f:
        sock = urllib2.urlopen('http://www.fundamentus.com.br/detalhes.php?papel='+line.strip())
        htmlSource = sock.read()
        sock.close()

        parser = MyHTMLParser()
        parser.feed(htmlSource)

        P = parser.cotacao[parser.cotacao.index('Cotação')+1]
        VM = parser.cotacao[parser.cotacao.index('Valor de mercado')+1]
        VP = parser.cotacao[parser.cotacao.index('Patrim. L\xedq')+1]
        AC = parser.cotacao[parser.cotacao.index('Ativo Circulante')+1]
        A = parser.cotacao[parser.cotacao.index('Ativo')+1]
        PL = parser.cotacao[parser.cotacao.index('P/L')+1]
        PVP = parser.cotacao[parser.cotacao.index('P/VP')+1]
        PCG = parser.cotacao[parser.cotacao.index('P/Cap. Giro')+1]
        PAC = parser.cotacao[parser.cotacao.index('P/Ativ Circ Liq')+1]
        DY = parser.cotacao[parser.cotacao.index('Div. Yield')+1]
        ROIC = parser.cotacao[parser.cotacao.index('ROIC')+1]
        ROE = parser.cotacao[parser.cotacao.index('ROE')+1]
        CR = parser.cotacao[parser.cotacao.index('Cres. Rec (5a)')+1]
        DBPL = parser.cotacao[parser.cotacao.index('Div Br/ Patrim')+1]

        VP = float(VP.replace('.', ''))
        AC = float(AC.replace('.', ''))
        A = float(A.replace('.', ''))
        VM = float(VM.replace('.', ''))

        grah = str(VM/(AC-A+VP)).replace('.', ',')

        papel = parser.cotacao[parser.cotacao.index('Papel')+1]
        print papel,

        mix = papel + ' ' + str(P) + ' ' + grah + ' ' + str(PL) + ' ' + str(PVP) + ' ' + str(PCG) + ' ' + str(PAC) + ' ' + str(DY) + ' ' + str(ROIC) + ' ' + str(ROE) + ' ' + str(CR) + ' ' + str(DBPL) + '\n'
        g.write(mix)
        g.flush()
g.close()
#print parser.cotacao
