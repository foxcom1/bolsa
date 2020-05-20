#!/usr/local/bin/python
# -*- coding: iso-8859-1 -*-

###Estou fazendo a classe cotacoes. criando os campos para o banco de
###dados a partir do layout de cotaçoes historicas e do
###DemoCotacoesHistoricas.
##14/10/08

###Acabei com o metodo de preenchimento da tabela cotacoes com dados
###da bovespa.
##29/10/08

##Estou agora implementando a classe de indicadores. Estou redefinindo
##os modelos de balanço e demonstrativo para tentar reunir os 2 tipos
##de cada (bp e bp2) e (dr e dr2) em um só (bp e dr). Também estou
##vendo como posso atualizar automaticamente a partir do site da cvm.
##Deixei a tarefa de conseguir o login/senha com André. Ainda faltam
##alguns históricos, como o historico do número de ações de cada
##empresas. 01/11/08

##Acabei de especificar o bp e dr tipo 1. Falta o tipo 2. A
##atualizaçao automatica atraves do download multiplo foi pro
##espaço... pois é preciso cnpj. terei que fazer tudo manualmente.
##henrique felcar concordou em associarmos esforços e estamos baixando
##todos os balancos do www.fundamentus.com. Ainda falta tentar
##especificar o tipo 2 e tentar unificar as 2 tabelas. 07/12/08

##Modifiquei o nome da classe principal de Bovespa para Graham e
##substitui as classes Inflacao, Dolar, Cotacoes e Indicadores por
##funcoes na classe Graham. Tambem eliminei a classe ConnectDB, pois
##agora ha apenas uma classe para conectar. Especifiquei o tipo 2, mas
##ainda nao unifiquei as notacoes. Talvez nao valha a pena... Melhor
##completar os calculos dos indicadores e do fluxo de caixa
##descontado. Depois vejo o que posso melhorar. Comecei a implementar
##updateDolar, que atualiza cotacao diaria do dolar automaticamente
##pela internet. Onde coloco o self.cota? Agora nao sao 4 tabelas: BP,
##DR, BP2 e DR2. Sao 2: BP e DR formam ITR1 e BP2 e DR2 formam ITR2.
##Alterar fillITR para refletir mudancas. CS e necessario? Fica como
##esta? 17/12/08

##Ao CS foi adicionado um dado 'itr' para especificar em qual tabela
##(ITR1 ou ITR2) estara o banco de dados BP/DR. Fiz algumas alteracoes
##em fillITR para acomodar as mudancas anteriores. Aparentemente esta
##funcionando. Baixei o historico do ipca da pagina do ibge em excel,
##mas o formato estava pouco pratico. Entao criei uma nova tabela com
##os dados (ipca3.xls). A atualizacao do ipca, por ser mensal, pode
##ser feita manualmente. Ja a cotacao do dolar devera ser automatica,
##assim como as cotacoes das acoes. Falta implementar as atualizacoes
##das cotacoes do dolar e das acoes e o metodo fillInflacao, alem de
##um modo de apdate manual para a inflacao. Tambem faltam o calculo
##dos indicadores (inclusive FCD) e alguns dados que ainda nao
##descobri como baixar de modo mais facil, como os dividendos e o
##numero de acoes (tudo historico). Pelo programa DIVEXT 8.2 (da CVM)
##eh uma desgraca; nunca vi programa tao mal-feito. Ha a possibilidade
##do proprio site fundamentus ou infoinvest, aparentemente muito bom,
##mas ainda versao beta (falta explorar mais). 05/02/09

"""Bovespa's companies database utility."""

import sys
import zipfile
import os
#import glob
import re
import fnmatch
import xlrd
import MySQLdb
from datetime import date
from HTMLParser import HTMLParser
import urllib2

###################################
## BALANCO PATRIMONIAL -- TIPO 1 ##
###################################
##1	Ativo Total
##1.01	Ativo Circulante
##1.01.01	Disponibilidades
##1.01.02	Créditos
##1.01.03	Estoques
##1.01.04	Outros
##1.02.01	Ativo Realizável a Longo Prazo
##1.02.01.01	Créditos Diversos
##1.02.01.02	Créditos com Pessoas Ligadas
##1.02.01.03	Outros
##1.02.02	Ativo Permanente
##1.02.02.01	Investimentos
##1.02.02.02	Imobilizado
##1.02.02.03	Intangível
##1.02.02.04	Diferido
##2	Passivo Total
##2.01	Passivo Circulante
##2.01.01	Empréstimos e Financiamentos
##2.01.02	Debêntures
##2.01.03	Fornecedores
##2.01.04	Impostos, Taxas e Contribuições
##2.01.05	Dividendos a Pagar
##2.01.06	Provisões
##2.01.07	Dívidas com Pessoas Ligadas
##2.01.08	Outros
##2.02.01	Passivo Exigível a Longo Prazo
##2.02.01.01	Empréstimos e Financiamentos
##2.02.01.02	Debêntures
##2.02.01.03	Provisões
##2.02.01.04	Dívidas com Pessoas Ligadas
##2.02.01.05	Adiantamento para Futuro Aumento Capital
##2.02.01.06	Outros
##2.02.02	Resultados de Exercícios Futuros
##2.04	Patrimônio Líquido
##2.04.01	Capital Social Realizado
##2.04.02	Reservas de Capital
##2.04.03	Reservas de Reavaliação
##2.04.04	Reservas de Lucro
##2.04.05	Lucros/Prejuízos Acumulados
##2.04.06	Adiantamento para Futuro Aumento Capital
##########################################
## DEMONSTRATIVO DE RESULTADOS - TIPO 1 ##
##########################################
##3.01	Receita Bruta de Vendas e/ou Serviços
##3.02	Deduções da Receita Bruta
##3.03	Receita Líquida de Vendas e/ou Serviços
##3.04	Custo de Bens e/ou Serviços Vendidos
##3.05	Resultado Bruto
##3.06	Despesas/Receitas Operacionais
##3.06.01	Com Vendas
##3.06.02	Gerais e Administrativas
##3.06.03	Financeiras
##3.06.04	Outras Receitas Operacionais
##3.06.05	Outras Despesas Operacionais
##3.06.06	Resultado da Equivalência Patrimonial
##3.07	Resultado Operacional
##3.08	Resultado Não Operacional
##3.09	Resultado Antes Tributação/Participações
##3.10	Provisão para IR e Contribuição Social
##3.11	IR Diferido
##3.12	Participações/Contribuições Estatutárias
##3.13	Reversão dos Juros sobre Capital Próprio
##3.14	Part. de Acionistas Não Controladores
##3.15	Lucro/Prejuízo do Período

ITR1BP = """\
id SMALLINT UNSIGNED NOT NULL,\
Trimestre   DATE NOT NULL,\
`1`	        DECIMAL(13,3),\
`1.01`	        DECIMAL(13,3),\
`1.01.01`	DECIMAL(13,3),\
`1.01.02`	DECIMAL(13,3),\
`1.01.03`       DECIMAL(13,3),\
`1.01.04`       DECIMAL(13,3),\
`1.02.01`	DECIMAL(13,3),\
`1.02.01.01`    DECIMAL(13,3),\
`1.02.01.02`    DECIMAL(13,3),\
`1.02.01.03`    DECIMAL(13,3),\
`1.02.02`	DECIMAL(13,3),\
`1.02.02.01`    DECIMAL(13,3),\
`1.02.02.02`    DECIMAL(13,3),\
`1.02.02.03`    DECIMAL(13,3),\
`1.02.02.04`    DECIMAL(13,3),\
`2`	        DECIMAL(13,3),\
`2.01`	        DECIMAL(13,3),\
`2.01.01`       DECIMAL(13,3),\
`2.01.02`       DECIMAL(13,3),\
`2.01.03`	DECIMAL(13,3),\
`2.01.04`       DECIMAL(13,3),\
`2.01.05`       DECIMAL(13,3),\
`2.01.06`	DECIMAL(13,3),\
`2.01.07`       DECIMAL(13,3),\
`2.01.08`	DECIMAL(13,3),\
`2.02.01`	DECIMAL(13,3),\
`2.02.01.01`    DECIMAL(13,3),\
`2.02.01.02`    DECIMAL(13,3),\
`2.02.01.03`    DECIMAL(13,3),\
`2.02.01.04`    DECIMAL(13,3),\
`2.02.01.05`    DECIMAL(13,3),\
`2.02.01.06`    DECIMAL(13,3),\
`2.02.02`       DECIMAL(13,3),\
`2.04`	        DECIMAL(13,3),\
`2.04.01`       DECIMAL(13,3),\
`2.04.02`	DECIMAL(13,3),\
`2.04.03`       DECIMAL(13,3),\
`2.04.04`       DECIMAL(13,3),\
`2.04.05`	DECIMAL(13,3),\
`2.04.06`	DECIMAL(13,3)\
"""
ITR1DR = """\
`3.01`	DECIMAL(15,3),\
`3.02`	DECIMAL(15,3),\
`3.03`	DECIMAL(15,3),\
`3.04`	DECIMAL(15,3),\
`3.05`	DECIMAL(15,3),\
`3.06`	DECIMAL(15,3),\
`3.06.01`	DECIMAL(15,3),\
`3.06.02`	DECIMAL(15,3),\
`3.06.03`	DECIMAL(15,3),\
`3.06.04`	DECIMAL(15,3),\
`3.06.05`	DECIMAL(15,3),\
`3.06.06`	DECIMAL(15,3),\
`3.07`	DECIMAL(15,3),\
`3.08`	DECIMAL(15,3),\
`3.09`	DECIMAL(15,3),\
`3.10`	DECIMAL(15,3),\
`3.11`	DECIMAL(15,3),\
`3.12`	DECIMAL(15,3),\
`3.13`	DECIMAL(15,3),\
`3.14`	DECIMAL(15,3),\
`3.15`	DECIMAL(15,3)\
"""
ITR1 = ITR1BP + ',' + ITR1DR

###################################
## BALANCO PATRIMONIAL -- TIPO 2 ##
###################################
##1	Ativo Total
##1.01	Ativo Circulante
##1.01.01	Disponibilidades
##1.01.02	Aplicações Interfinanceiras de Liquidez
##1.01.03	Títulos e Valores Mobiliários
##1.01.04	Relações Interfinanceiras
##1.01.05	Relações Interdependências
##1.01.06	Operações de Crédito
##1.01.07	Operações de Arrendamento Mercantil
##1.01.08	Outros Créditos
##1.01.09	Outros Valores e Bens
##1.02	Ativo Realizável a Longo Prazo
##1.02.01	Aplicações Interfinanceiras de Liquidez
##1.02.02	Títulos e Valores Mobiliários
##1.02.03	Relações Interfinanceiras
##1.02.04	Relações Interdependências
##1.02.05	Operações de Crédito
##1.02.06	Operações de Arrendamento Mercantil
##1.02.07	Outros Créditos
##1.02.08	Outros Valores e Bens
##1.03	Ativo Permanente
##1.03.01	Investimentos
##1.03.02	Imobilizado de Uso
##1.03.03	Imobilizado de Arrendamento
##1.03.04	Intangível
##1.03.05	Diferido
##2	Passivo Total
##2.01	Passivo Circulante
##2.01.01	Depósitos
##2.01.02	Captações no Mercado Aberto
##2.01.03	Recursos de Aceites e Emissão de Títulos
##2.01.04	Relações Interfinanceiras
##2.01.05	Relações Interdependências
##2.01.06	Obrigações por Empréstimos
##2.01.07	Obrigações por Repasse do País
##2.01.08	Obrigações por Repasse do Exterior
##2.01.09	Outras Obrigações
##2.02	Passivo Exigível a Longo Prazo
##2.02.01	Depósitos
##2.02.02	Captações no Mercado Aberto
##2.02.03	Recursos de Aceites e Emissão de Títulos
##2.02.04	Relações Interfinanceiras
##2.02.05	Relações Interdependências
##2.02.06	Obrigações por Empréstimos
##2.02.07	Obrigações por Repasse do País
##2.02.08	Obrigações por Repasse do Exterior
##2.02.09	Outras Obrigações
##2.03	Resultados de Exercícios Futuros
##2.04	Part. de Acionistas Não Controladores
##2.05	Patrimônio Líquido
##2.05.01	Capital Social Realizado
##2.05.02	Reservas de Capital
##2.05.03	Reservas de Reavaliação
##2.05.04	Reservas de Lucro
##2.05.05	Ajustes de Títulos e Valores Mobiliários
##2.05.06	Lucros/Prejuízos Acumulados
##########################################
## DEMONSTRATIVO DE RESULTADOS - TIPO 2 ##
##########################################
##3.01	Receitas da Intermediação Financeira
##3.02	Despesas da Intermediação Financeira
##3.03	Resultado Bruto Intermediação Financeir
##3.04	Outras Despesas/Receitas Operacionais
##3.04.01	Receitas de Prestação de Serviços
##3.04.02	Despesas de Pessoal
##3.04.03	Outras Despesas Administrativas
##3.04.04	Despesas Tributárias
##3.04.05	Outras Receitas Operacionais
##3.04.06	Outras Despesas Operacionais
##3.04.07	Resultado da Equivalência Patrimonial
##3.05	Resultado Operacional
##3.06	Resultado Não Operacional
##3.06.01	Receitas
##3.06.02	Despesas
##3.07	Resultado Antes Tributação/Participaçõe
##3.08	Provisão para IR e Contribuição Social
##3.09	IR Diferido
##3.10	Participações/Contribuições Estatutária
##3.10.01	Participações
##3.10.02	Contribuições
##3.11	Reversão dos Juros sobre Capital Própri
##3.12	Part. de Acionistas Não Controladores
##3.13	Lucro/Prejuízo do Período



ITR2BP = """\
id	SMALLINT UNSIGNED NOT NULL,\
Trimestre	DATE NOT NULL,\
`1`	DECIMAL(13,3),\
`1.01`	DECIMAL(13,3),\
`1.01.01`	DECIMAL(13,3),\
`1.01.02`	DECIMAL(13,3),\
`1.01.03`	DECIMAL(13,3),\
`1.01.04`	DECIMAL(13,3),\
`1.01.05`	DECIMAL(13,3),\
`1.01.06`	DECIMAL(13,3),\
`1.01.07`	DECIMAL(13,3),\
`1.01.08`	DECIMAL(13,3),\
`1.01.09`	DECIMAL(13,3),\
`1.02`	DECIMAL(13,3),\
`1.02.01`	DECIMAL(13,3),\
`1.02.02`	DECIMAL(13,3),\
`1.02.03`	DECIMAL(13,3),\
`1.02.04`	DECIMAL(13,3),\
`1.02.05`	DECIMAL(13,3),\
`1.02.06`	DECIMAL(13,3),\
`1.02.07`	DECIMAL(13,3),\
`1.02.08`	DECIMAL(13,3),\
`1.03`	DECIMAL(13,3),\
`1.03.01`	DECIMAL(13,3),\
`1.03.02`	DECIMAL(13,3),\
`1.03.03`	DECIMAL(13,3),\
`1.03.04`	DECIMAL(13,3),\
`1.03.05`	DECIMAL(13,3),\
`2`	DECIMAL(13,3),\
`2.01`	DECIMAL(13,3),\
`2.01.01`	DECIMAL(13,3),\
`2.01.02`	DECIMAL(13,3),\
`2.01.03`	DECIMAL(13,3),\
`2.01.04`	DECIMAL(13,3),\
`2.01.05`	DECIMAL(13,3),\
`2.01.06`	DECIMAL(13,3),\
`2.01.07`	DECIMAL(13,3),\
`2.01.08`	DECIMAL(13,3),\
`2.01.09`	DECIMAL(13,3),\
`2.02`	DECIMAL(13,3),\
`2.02.01`	DECIMAL(13,3),\
`2.02.02`	DECIMAL(13,3),\
`2.02.03`	DECIMAL(13,3),\
`2.02.04`	DECIMAL(13,3),\
`2.02.05`	DECIMAL(13,3),\
`2.02.06`	DECIMAL(13,3),\
`2.02.07`	DECIMAL(13,3),\
`2.02.08`	DECIMAL(13,3),\
`2.02.09`	DECIMAL(13,3),\
`2.03`	DECIMAL(13,3),\
`2.04`	DECIMAL(13,3),\
`2.05`	DECIMAL(13,3),\
`2.05.01`	DECIMAL(13,3),\
`2.05.02`	DECIMAL(13,3),\
`2.05.03`	DECIMAL(13,3),\
`2.05.04`	DECIMAL(13,3),\
`2.05.05`	DECIMAL(13,3),\
`2.05.06`	DECIMAL(13,3)\
"""
ITR2DR = """\
`3.01`	DECIMAL(15,3),\
`3.02`	DECIMAL(15,3),\
`3.03`	DECIMAL(15,3),\
`3.04`	DECIMAL(15,3),\
`3.04.01`	DECIMAL(15,3),\
`3.04.02`	DECIMAL(15,3),\
`3.04.03`	DECIMAL(15,3),\
`3.04.04`	DECIMAL(15,3),\
`3.04.05`	DECIMAL(15,3),\
`3.04.06`	DECIMAL(15,3),\
`3.04.07`	DECIMAL(15,3),\
`3.05`	DECIMAL(15,3),\
`3.06`	DECIMAL(15,3),\
`3.06.01`	DECIMAL(15,3),\
`3.06.02`	DECIMAL(15,3),\
`3.07`	DECIMAL(15,3),\
`3.08`	DECIMAL(15,3),\
`3.09`	DECIMAL(15,3),\
`3.10`	DECIMAL(15,3),\
`3.10.01`	DECIMAL(15,3),\
`3.10.02`	DECIMAL(15,3),\
`3.11`	DECIMAL(15,3),\
`3.12`	DECIMAL(15,3),\
`3.13`	DECIMAL(15,3)\
"""
ITR2 = ITR2BP + ',' + ITR2DR

def parseDate(d):
    """Convert dd/mm/yyyy string to tuple (yyyy, mm, dd).

    """
    return (d.split('/')[::-1])


def addDot(s):
    """Add a decimal point to string of numbers.

    """
    return (s[:-2]+'.'+ s[-2:])


def unzip_files(dirdb):
    """Open all zip files in dirdb directory,
    extract xls file from each one onto temporary file,
    then rename it to company name.

    """
    file_name_temp = os.path.join(dirdb, 'file_temp.xls') #join complete path for temp file

    fileList = list(Globcp().iglobcp(os.path.join(dirdb, '*.zip'))) #list complete path for zip files

    for zfile in fileList:
        zfobj = zipfile.ZipFile(zfile) #open zipfile object from zip file
        try:
            outfile = open(file_name_temp, 'wb') #open file to where it will be extract
            try:
                outfile.write(zfobj.read(zfobj.namelist()[0])) #read zip and extract it into outfile
            finally:
                outfile.close()
        except IOError:
            pass
        
        wb = xlrd.open_workbook(file_name_temp) #open excel file
        sh = wb.sheet_by_index(0) #get sheet where is company name
        cell_B1 = sh.cell(rowx=0, colx=1).value #get cell where is company name
        comp_name = cell_B1.split('-')[-1][:8].strip().replace(' ', '_') #get company name, max 8 characters, and replace empty space to '_'.
        comp_name = os.path.join(dirdb, comp_name.encode('Latin-1') + '.xls') #join all path

        if os.path.isfile(comp_name): #if file exists:
            comp_name = os.path.splitext(comp_name)[0] + '(2)' + '.xls' #add a number (2) at the end of namefile
            i = 2
            while os.path.isfile(comp_name): #if file still exists:
                i += 1
                comp_name = os.path.splitext(comp_name)[0][:-3] + '(%d)' % i + '.xls' #add a new number (i) at the end of filename
#        print file_name_temp, comp_name
        os.rename(file_name_temp, comp_name) #rename excel file

        zfobj.close()    


class Error(Exception):
    """Base class for exceptions in this module.

    """
    pass


class Globcp:
    """Glob module copy, but with some modifications
    to allow to search a list instead a simgle pathname.

    Example:
    > files = ['p*.xls', 'i*.xls']
    > instance = Globcp();
    > filenames = list(instance.iglobcp(files))
    > print filenames
    > ['PETROBR.xls', 'itausa.xls']

    """

    def __init__(self):
        self.magic_check = re.compile('[*?[]')

    def has_magic(self, s):
        return self.magic_check.search(s) is not None

    def iglobcp(self, listpath):
        """Return a list of paths matching a pathname pattern.

        The pattern may contain simple shell-style wildcards a la fnmatch.

        """
        for pathname in listpath:
            if not self.has_magic(pathname):
                if os.path.lexists(pathname):
                    yield pathname
                continue
            dirname, basename = os.path.split(pathname)
            if not dirname:
                for name in self.glob1(os.curdir, basename):
                    yield name
                continue
            if has_magic(dirname):
                dirs = iglobcp(dirname)
            else:
                dirs = [dirname]
            if self.has_magic(basename):
                glob_in_dir = glob1
            else:
                glob_in_dir = glob0
            for dirname in dirs:
                for name in glob_in_dir(dirname, basename):
                    yield os.path.join(dirname, name)

    # These 2 helper functions non-recursively glob inside a literal directory.
    # They return a list of basenames. `glob1` accepts a pattern while `glob0`
    # takes a literal basename (so it only has to check for its existence).

    def glob1(self, dirname, pattern):
        if not dirname:
            dirname = os.curdir
        try:
            names = os.listdir(dirname)
        except os.error:
            return []
        if pattern[0]!='.':
            names=filter(lambda x: x[0]!='.',names)
        return fnmatch.filter(names,pattern)

    def glob0(self, dirname, basename):
        if basename == '':
            # `os.path.split()` returns an empty basename for paths ending with a
            # directory separator.  'q*x/' should match only directories.
            if os.path.isdir(dirname):
                return [basename]
        else:
            if os.path.lexists(os.path.join(dirname, basename)):
                return [basename]
        return []

##class ConnectDB:
##    def __init__(self, myhost = 'localhost', myuser = 'root', mypasswd = 'blablabla', mydb = 'test'):
##        #connect to the MySQL server
##        try:
##            self.conn = MySQLdb.connect(host = myhost,
##                                        user = myuser,
##                                        passwd = mypasswd,
##                                        db = mydb)
##            self.cursor = self.conn.cursor()
##        except MySQLdb.Error, e:
##            print "Error %d: %s" % (e.args[0], e.args[1])
##            sys.exit (1)    

class MyHTMLParser(HTMLParser):
    def reset(self):
        HTMLParser.reset(self)
        self.sig = False
        self.sig2 = False
        self.cotacao = []

    def handle_comment(self, data):
        if data == 'conteudo':
            self.sig = True

    def handle_endtag(self, tag):
        if tag == 'table' and self.sig: self.sig = False

    def handle_data(self, data):
        if self.sig:
            if self.sig2:
                self.cotacao.append(str(data).strip())
            if data == 'Taxa de Venda':
                self.sig2 = True

class Graham:
    def __init__(self, myhost = 'localhost', myuser = 'root', mypasswd = 'blablabla', mydb = 'test'):
        try:
            self.__conn = MySQLdb.connect(host = myhost,
                                        user = myuser,
                                        passwd = mypasswd,
                                        db = mydb)
            self.cursor = self.__conn.cursor()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)

##        #connect to the MySQL server
##        connInstance = ConnectDB(myhost, myuser, mypasswd, mydb)
##        self.__conn = connInstance.conn
##        self.cursor = connInstance.cursor

##    def __setitem__(self, Mycompany, item):#ainda nao esta bem definida....
##        if key == "name" and item:
##            self.__parse(item)
##        FileInfo.__setitem__(self, Mycompany, item)
##
##    def __getitem__(self, Mycompany):#tb ainda nao esta bem definida...
##        return self.data[key]

    def findID(self, company):
            query = "SELECT id FROM CS WHERE (NomePregao LIKE \'%%%s%%\') OR (DenomSocial LIKE \'%%%s%%\')" % (company, company)
            self.cursor.execute(query)
            CSid = self.cursor.fetchall()
            #verify company(ies) found...
            if len(CSid) != 1: #more than one id was found (or none)
                if len(CSid) == 0 and len(company.split()) > 1: #search not found; large company name
                    for c in company.split(): #now split CompName and look for each one.
                        query = "SELECT id, Codigo FROM CS WHERE (NomePregao LIKE \'%%%s%%\') OR (DenomSocial LIKE \'%%%s%%\')" % (c, c)
                        self.cursor.execute(query)
                        CSid = self.cursor.fetchall()
                        if len(CSid) == 1: #only one company was found. Verifying...
                            while True:
                                ans = raw_input('Has %s code %s? (Y or N):' % (company, CSid[0][1]))
                                if ans in ('y', 'Y', 'n', 'N'): break
                            if ans in ('y', 'Y'): break #get the company!
                    if len(CSid) != 1: #ops... didn't get any company
                        print "ERROR: Insufficient information to get company name. CSid =  ", CSid, company
                        return
                elif len(CSid) == 0 and len(CompName.split()) == 1: #none was found
                    print "ERROR: Insufficient information to get company name. CSid = ", CSid, company
                    return  #sys.exit(1)
                elif len(CSid) > 1: #more than one company was found
                    query = "SELECT NomePregao FROM CS WHERE id in (%s)" % ','.join([str(a[0]) for a in CSid]) #show companies found
                    self.cursor.execute(query)
                    companyList = self.cursor.fetchall()
                    NomePregao = ','.join([str(a[0]) for a in companyList])
                    while True: #choose one company
                        ans = raw_input('Choose a company (%s): ' % NomePregao)
                        if (ans in NomePregao.split(',')) or (ans == ''): break #reposta vazia adicionada em 31/12/2008
                    if ans == '': print 'Next...'; return #adicionada em 31/12/2008
                    CSid = dict(zip(NomePregao.split(','), CSid))[ans],
            return CSid


    def createCS(self):
        """Create a table named CS in database specified at instance creation.
        CS contains:

        id - identification number - SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY
        NomePregao - small name (ex. PETROBRAS) - VARCHAR(12)
        Codigo - company code (ex. PETR) - CHAR(4)
        DenomSocial - full name (ex. PETROLEO BRASILEIRO S.A. PETROBRAS) - VARCHAR(50)
        SegMercado - market type (ex. NM) - VARCHAR(15)
        Capital - R$ - DECIMAL(15,3)
        Aprovado -  - DATE
        QtdOrd - Quantity ordinary stock - BIGINT
        QtdPref - Quantity preference stock - BIGINT
        QtdTot - Quantity total stock - BIGINT

        """
        try:
            self.cursor.execute("CREATE TABLE CS (\
                                 id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\
                                 itr TINYINT,\
                                 NomePregao VARCHAR(12),\
                                 Codigo CHAR(4),\
                                 DenomSocial VARCHAR(50),\
                                 SegMercado VARCHAR(15),\
                                 Capital DECIMAL(15,3),\
                                 Aprovado DATE,\
                                 QtdOrd BIGINT,\
                                 QtdPref BIGINT,\
                                 QtdTot BIGINT\
                                 )"
                                )
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)

    def fillCS(self, model):
        """Fill CS table with data from xls file <model>,
        took from <www.bovespa.com.br> (<CapitalSocial.xls>).

        """
        wb = xlrd.open_workbook(model) #open excel file
        try:
            sh = wb.sheet_by_index(0) #get first sheet, <balanço patrimonial>
            for i in range(2, sh.nrows): #run lines from 2nd line
                if sh.cell(rowx=i, colx=4).value == 'Homologado':
                    cell = [str(sh.cell(rowx=i, colx=j).value).strip()
                            for j in range(4)+range(5, 10)] #get first column
                    cell[5] = '-'.join(cell[5].split('/')[::-1]) #date to format 'yyyy-mm-dd'
                    self.cursor.execute("INSERT INTO CS (\
                                            NomePregao,\
                                            Codigo,\
                                            DenomSocial,\
                                            SegMercado,\
                                            Capital,\
                                            Aprovado,\
                                            QtdOrd,\
                                            QtdPref,\
                                            QtdTot\
                                            )\
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ", tuple(cell)
                                        ) #add columns into table)    

        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)    

    def createITR(self):
        """Create 2 tables: ITR1 and ITR2.

        """
        try:
            query = "CREATE TABLE ITR1 (%s, FOREIGN KEY(id) REFERENCES CS(id))" % (ITR1,)
            self.cursor.execute(query)

##            query = "CREATE TABLE BP2 (%s, FOREIGN KEY(id) REFERENCES CS(id))" % (BP2list,)
##            self.cursor.execute(query)

            query =  "CREATE TABLE ITR2 (%s, FOREIGN KEY(id) REFERENCES CS(id))" % (ITR2,)
            self.cursor.execute(query)

##            query =  "CREATE TABLE DR2 (%s, FOREIGN KEY(id) REFERENCES CS(id))" % (DR2list,)
##            self.cursor.execute(query)
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)

    def fillITR(self, dirdb = '.', fileList = ['b*.xls', 'p*.xls']):
        """Read all (.xls) files in <dirdb> specified in <fileList> and
        copy all data to mysql tables ITR1 or ITR2, depending on row number.

        """
    #    setChar = ',./ '    
        try:
            g = Globcp() #modified glob function to catch list of files like ['*.doc', '*.xls']
            fileList = list(g.iglobcp(fileList))

            for xfile in fileList:
                wb = xlrd.open_workbook(xfile) #open excel file
                sh = wb.sheet_by_index(0) #get first sheet, <balanço patrimonial>

                cell = sh.cell(rowx=0, colx=1).value #get company name
                CompName = cell.split('-')[-1].encode('Latin-1').strip()
                print CompName

                #get id to company from CS(id)
##                CSid = findID(CompName)
                query = "SELECT id FROM CS WHERE (NomePregao LIKE \'%%%s%%\') OR (DenomSocial LIKE \'%%%s%%\')" % (CompName, CompName)
                self.cursor.execute(query)
                CSid = self.cursor.fetchall()
                #verify company(ies) found...
                if len(CSid) != 1: #more than one id was found (or none)
                    if len(CSid) == 0 and len(CompName.split()) > 1: #search not found; large company name
                        for c in CompName.split(): #now split CompName and look for each one.
                            query = "SELECT id, Codigo FROM CS WHERE (NomePregao LIKE \'%%%s%%\') OR (DenomSocial LIKE \'%%%s%%\')" % (c, c)
                            self.cursor.execute(query)
                            CSid = self.cursor.fetchall()
                            if len(CSid) == 1: #only one company was found. Verifying...
                                while True:
                                    ans = raw_input('Has %s code %s? (Y or N):' % (CompName, CSid[0][1]))
                                    if ans in ('y', 'Y', 'n', 'N'): break
                                if ans in ('y', 'Y'): break #get the company!
                        if len(CSid) != 1: #ops... didn't get any company
                            print "ERROR: Insufficient information to get company name. CSid =  ", CSid, CompName
                            continue
                    elif len(CSid) == 0 and len(CompName.split()) == 1: #none was found
                        print "ERROR: Insufficient information to get company name. CSid = ", CSid, CompName
                        continue  #sys.exit(1)
                    elif len(CSid) > 1: #more than one company was found
                        query = "SELECT NomePregao FROM CS WHERE id IN (%s)" % ','.join([str(a[0]) for a in CSid]) #show companies found
                        self.cursor.execute(query)
                        CompNameList = self.cursor.fetchall()
                        NomePregao = ','.join([str(a[0]) for a in CompNameList])
                        while True: #choose one company
                            ans = raw_input('Choose a company (%s): ' % NomePregao)
                            if (ans in NomePregao.split(',')) or (ans == ''): break #reposta vazia adicionada em 31/12/2008
                        if ans == '': print 'Next...'; continue #adicionada em 31/12/2008
                        CSid = dict(zip(NomePregao.split(','), CSid))[ans],
                #ok, we got the company. now to the data
                for n in (0, 1):
                    sh = wb.sheet_by_index(n) #get sheet 0 (Balanco Patrimonial)
                    nrows = sh.nrows
                    if nrows in (42, 23):
                        itr_type = 1 #auxiliar para enxugar o codigo
                        NamCols = [a.split()[0]
                                   for a in (ITR1BP, ITR1DR)[n].split(',')
                                       if len(a.split())>1] #get only 1st column: [id, Trimestre, `1`, `1.01`,...]
                    elif nrows in (58, 26):
                        itr_type = 2
                        NamCols = [a.split()[0]
                                   for a in (ITR2BP, ITR2DR)[n].split(',')
                                       if len(a.split())>1] #get only 1st column: [id, Trimestre, `1`, `1.01`,...]
                    else:
                        print "ERROR: Column count doesn't match value count at row 1. ", CompName
                        break

                    if n == 1:
                        NamCols.insert(0, 'Trimestre')
                        NamCols.insert(0, 'id')

                    for j in range(sh.ncols-1, 0, -1): #run columns j
                        col = sh.col_values(j) #get column j
                        if col[1] == '': continue #no data
                        col[0] = CSid[0][0] #id element
                        col[1] = '\'' + '-'.join(str(col[1]).split('/')[::-1]) + '\'' #u'dd/mm/yyyy' --> "'yyyy-mm-dd'"
                        while col.count(''): col[col.index('')] = 0.0 #replace all: '' --> 0.0
                        col = [str(a) for a in col] #unicode --> string

                        query = "SELECT id FROM ITR%d\
                                 WHERE id=%s AND Trimestre=%s" % (itr_type, col[0], col[1]) #already in database?
                        self.cursor.execute(query)
                        if len(self.cursor.fetchall()) == 0: #still no data for company in (on? at? for?) that period
                                query = "INSERT INTO ITR%d (%s)\
                                         VALUES (%s)" % (itr_type, ', '.join(NamCols), ', '.join(col))
                                self.cursor.execute(query)
                                query = "UPDATE CS\
                                         SET ITR = %d\
                                         WHERE id = %s" % (itr_type, col[0])
                                self.cursor.execute(query)
                        else: #data already in database
                            query = "SELECT id FROM ITR%d\
                                     WHERE id=%s AND Trimestre=%s AND `%s` IS NOT NULL" % (itr_type, col[0], col[1], ('1', '3.01')[n]) #already in database?
                            self.cursor.execute(query)
                            if len(self.cursor.fetchall()) == 0: #o (BP, DR)[n] ainda nao foi preenchido, mas ha dado da empresa referente a esse periodo
                                query = "UPDATE ITR%d\
                                         SET %s\
                                         WHERE id = %s AND Trimestre = %s" % (itr_type, ', '.join(['%s=%s' % (a,b) for a,b in zip(NamCols, col)]), col[0], col[1])
                                self.cursor.execute(query)
                                print 'update', col[1]
                            else: #os dados ja existem no banco
                                while True:
                                    ans = raw_input("Update %s from %s (%s) into table? (y or n): " % (('Balanco Patrimonial', 'Demonstrativo de Resultados')[n], CompName, col[1]))
                                    if ans in ('y', 'Y', 'n', 'N'): break
                                if ans in ('y', 'Y'):
                                    query = "UPDATE ITR%d\
                                             SET %s\
                                             WHERE id = %s AND Trimestre = %s" % (itr_type, ', '.join(['%s=%s' % (a,b) for a,b in zip(NamCols, col)]), col[0], col[1])
                                    self.cursor.execute(query)

        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)

    def update(self):
        self.__conn.commit()

    def close(self):
        self.cursor.close()
        self.__conn.commit()
        self.__conn.close()
    

    def createInflacao(self):
        """..

        """
        try:
            self.cursor.execute("CREATE TABLE Inflacao (\
                                 Ano SMALLINT(4) UNSIGNED NOT NULL PRIMARY KEY,\
                                 Jan DECIMAL(4,2),\
                                 Fev DECIMAL(4,2),\
                                 Mar DECIMAL(4,2),\
                                 Abr DECIMAL(4,2),\
                                 Mai DECIMAL(4,2),\
                                 Jun DECIMAL(4,2),\
                                 Jul DECIMAL(4,2),\
                                 Ago DECIMAL(4,2),\
                                 Sep DECIMAL(4,2),\
                                 Oct DECIMAL(4,2),\
                                 Nov DECIMAL(4,2),\
                                 Dez DECIMAL(4,2),\
                                 Acumulado DECIMAL(6,4)\
                                 )"
                                )
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)
        
    def fillInflacao(self, filepath):
        """..

        """
        wb = xlrd.open_workbook(filepath) #open excel file
        try:
            sh = wb.sheet_by_index(0)
#            cell = sh.cell(rowx=0, colx=0).value
            print sh.nrows
            for i in range(sh.nrows):
                cell = sh.cell(rowx=i, colx=0).value
                print cell,

##            for i in range(1, sh.nrows): #run lines from 2nd line
##                cell = [str(sh.cell(rowx=i, colx=j).value)
##                        for j in range(14)] #get first column
##                cell[0] = cell[0][:4]
## #               print cell
##                self.cursor.execute("INSERT INTO Inflacao (Ano, Jan, Fev, Mar, Abr, Mai, Jun, Jul, Ago, Sep, Oct, Nov, Dez, Acumulado) \
##                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ", tuple(cell)
##                                    ) #add columns into table)    

        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)    

    def updateInflacao(self):
        pass

    def createDolar(self):
        """Create a table named CS in database specified at instance creation.
        CS contains:

        """
        try:
            self.cursor.execute("CREATE TABLE Dolar (\
                                 id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\
                                 Data DATE NOT NULL,\
                                 Compra DECIMAL(5,4),\
                                 Venda DECIMAL(5,4)\
                                 )"
                                )
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)
        
    def fillDolar(self, filepath):
        """Fill CS table with data from xls file <model>,
        took from <www.bovespa.com.br> (<CapitalSocial.xls>).

        """
        wb = xlrd.open_workbook(filepath) #open excel file
        try:
            sh = wb.sheet_by_index(0) #get first sheet, <balanço patrimonial>

            for i in range(2, sh.nrows): #run lines from 2nd line
                cell = [str(sh.cell(rowx=i, colx=j).value)
                        for j in (0, 1, 2)] #get first column
                cell[0] = date(*xlrd.xldate_as_tuple(float(cell[0]), 0)[:3]).isoformat() #date to format 'yyyy-mm-dd'
#                print cell
                self.cursor.execute("INSERT INTO Dolar (\
                                        Data,\
                                        Compra,\
                                        Venda\
                                        )\
                                    VALUES (%s, %s, %s) ", tuple(cell)
                                    ) #add columns into table)    

        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)

    def updateDolar(self):
        sock = urllib2.urlopen('http://www4.bcb.gov.br/pec/taxas/batch/taxas.asp?id=txdolar&id=txdolar')
        htmlSource = sock.read()
        sock.close()

        parser = MyHTMLParser()
        parser.feed(htmlSource)
        parser.cotacao[0] = '-'.join(parser.cotacao[0].split('/')[::-1]) #dd/mm/yyyy --> yyyy-mm-dd
        parser.cotacao[1] = parser.cotacao[1].replace(',', '.') #n,nnn --> n.nnn
        parser.cotacao[2] = parser.cotacao[2].replace(',', '.') #n,nnn --> n.nnn
        self.cursor.execute("INSERT INTO Dolar (\
                                Data,\
                                Compra,\
                                Venda\
                                )\
                            VALUES (%s, %s, %s) ", tuple(parser.cotacao)
                            ) #add columns into table)    



##    def createCotacoes(self):
##        """Create a table named Cotacoes in database specified at instance creation.
##        Cotacoes contains:
##             Data DATE NOT NULL, data do pregão; formato "AAAAMMDD".
##             CodBDI TINYINT(2), código BDI; utilizado para classificar os papéis na emissão do boletim diário de informações; ver tabela anexa.
##             CodNeg VARCHAR(12), código de negociação do papel.
##             TpMerc TINYINT(2), tipo de mercado; código do mercado em que o papel está cadastrado; ver tabela anexa.
##             NomRes VARCHAR(12), nome resumido da empresa emissora do papel.
##             Especi VARCHAR(10), especificação do papel; ver tabela anexa.
##             PrazoT SMALLINT(3), prazo em dias do mercado a termo.
##             ModRef VARCHAR(4), moeda de referência; moeda usada na data do pregão.
##             PreAbe DECIMAL(13,2), preço de abertura do papel-mercado no pregão.
##             PreMax DECIMAL(13,2), preço máximo do papel-mercado no pregão.
##             PreMin DECIMAL(13,2), preço mínimo do papel-mercado no pregão.
##             PreMed DECIMAL(13,2), preço médio do papel-mercado no pregão.
##             PreUlt DECIMAL(13,2), preço do último negócio do papel-mercado no pregão.
##             PreOfC DECIMAL(13,2), preço da melhor oferta de compra do papel-mercado.
##             PreOfV DECIMAL(13,2), preço da melhor oferta de venda do papel-mercado.
##             TotNeg MEDIUMINT(5), número de negócios efetuados com o papel-mercado no pregão.
##             QuaTot BIGINT, quantidade total de títulos negociados neste papel-mercado.
##             VolTot DECIMAL(18,2), volume total de títulos negociados neste papel-mercado.
##             PreExe DECIMAL(13,2), preço de exercício para o mercado de opções ou valor do contrato para o mercado de termo secundário.
##             IndOpc TINYINT(1), indicador de correção de preços de exercícios ou valores de contrato para os mercados de opções ou termo secundário; ver tabela anexa.
##             DatVen DATE, data do vencimento para os mercados de opções ou termo secundário; formato "AAAAMMDD".
##             FatCot SMALLINT, fator de cotação do papel; '1' = cotação unitária; '1000' = cotação por lote de mil ações.
##             PtoExe DECIMAL(13,6), preço de exercício em pontos para opções referenciadas em dólar ou valor de contrato em pontos para termo secundário; para os referenciados em dólar, cada ponto equivale ao valor, na moeda corrente, de um centésimo da taxa média do dólar comercial interbancário de fechamento do dia anterior, ou seja, 1 ponto = 0,01 USD.
##             CodISI VARCHAR(12), código do papel no sistema isin ou código interno do papel; código do papel no sistema isin a partir de 15/05/1995.
##             DisMes SMALLINT(3), número de distribuição do papel; número de sequência do pepal correspondente ao estado de direito vigente.
##
##        """
##        try:
##            self.cursor.execute("CREATE TABLE Cotacoes (\
##                                 Data DATE NOT NULL,\
##                                 CodBDI CHAR(2),\
##                                 CodNeg CHAR(12),\
##                                 TpMerc CHAR(3),\
##                                 NomRes CHAR(12),\
##                                 Especi CHAR(10),\
##                                 PrazoT CHAR(3),\
##                                 ModRef CHAR(4),\
##                                 PreAbe CHAR(13),\
##                                 PreMax CHAR(13),\
##                                 PreMin CHAR(13),\
##                                 PreMed CHAR(13),\
##                                 PreUlt CHAR(13),\
##                                 PreOfC CHAR(13),\
##                                 PreOfV CHAR(13),\
##                                 TotNeg CHAR(5),\
##                                 QuaTot CHAR(18),\
##                                 VolTot CHAR(18),\
##                                 PreExe CHAR(13),\
##                                 IndOpc CHAR(1),\
##                                 DatVen CHAR(8),\
##                                 FatCot CHAR(7),\
##                                 PtoExe CHAR(13),\
##                                 CodISI CHAR(12),\
##                                 DisMes CHAR(3)\
##                                 )"
##                                )
##        except MySQLdb.Error, e:
##            print "Error %d: %s" % (e.args[0], e.args[1])
##            sys.exit (1)
##
##    def fillCotacoes(self, filename):
##        """Fill Cotacoes table with data from txt file took from <www.bovespa.com.br>.
##
##        """
##        self.cota = {'Data':'',
##                'CodBDI':'',
##                'CodNeg':'',
##                'TpMerc':'',
##                'NomRes':'',
##                'Especi':'',
##                'PrazoT':'',
##                'ModRef':'',
##                'PreAbe':'',
##                'PreMax':'',
##                'PreMin':'',
##                'PreMed':'',
##                'PreUlt':'',
##                'PreOfC':'',
##                'PreOfV':'',
##                'TotNeg':'',
##                'QuaTot':'',
##                'VolTot':'',
##                'PreExe':'',
##                'IndOpc':'',
##                'DatVen':'',
##                'FatCot':'',
##                'PtoExe':'',
##                'CodISI':'',
##                'DisMes':''}
##
##        txt = open(filename, 'r') #open txt file
##        try:
##            while True:
##                TipReg = txt.read(2)
##                if TipReg == '01': #cotacoes
##                    self.cota['Data'] = '-'.join(parseDate(txt.read(8)))
##                    self.cota['CodBDI'] = txt.read(2)
##                    self.cota['CodNeg'] = txt.read(12)
##                    self.cota['TpMerc'] = txt.read(3)
##                    self.cota['NomRes'] = txt.read(12)
##                    self.cota['Especi'] = txt.read(10)
##                    self.cota['PrazoT'] = txt.read(3)
##                    self.cota['ModRef'] = txt.read(4)
##                    self.cota['PreAbe'] = txt.read(13)
##                    self.cota['PreMax'] = txt.read(13)
##                    self.cota['PreMin'] = txt.read(13)
##                    self.cota['PreMed'] = txt.read(13)
##                    self.cota['PreUlt'] = txt.read(13)
##                    self.cota['PreOfC'] = txt.read(13)
##                    self.cota['PreOfV'] = txt.read(13)
##                    self.cota['TotNeg'] = txt.read(5)
##                    self.cota['QuaTot'] = txt.read(18)
##                    self.cota['VolTot'] = txt.read(18)
##                    self.cota['PreExe'] = txt.read(13)
##                    self.cota['IndOpc'] = txt.read(1)
##                    self.cota['DatVen'] = txt.read(8)
##                    self.cota['FatCot'] = txt.read(7)
##                    self.cota['PtoExe'] = txt.read(13)
##                    self.cota['CodISI'] = txt.read(12)
##                    self.cota['DisMes'] = txt.read(3)
##                    txt.read(1) #'\n'
##
##                    query = "INSERT INTO Cotacoes (%s)\
##                                VALUES %s" % (','.join(self.cota.keys()), str(tuple(self.cota.values())))
##                    self.cursor.execute(query)
##
##                elif TipReg == '00': txt.readline() #header
##                elif TipReg == '99': break #trailer
##                else: raise Error #need to define a TipRegError exception class
##                
##        except MySQLdb.Error, e:
##            print "Error %d: %s" % (e.args[0], e.args[1])
##            sys.exit (1)    
##
##    def updateCotacoes(self):
##        pass

    def createCotacoes(self):
        """Create a table named Cotacoes in database specified at instance creation.
        Cotacoes contains:
             Data DATE NOT NULL, data do pregão; formato "AAAAMMDD".
             Cod VARCHAR(12), código de negociação do papel.
             Aber DECIMAL(13,2), preço de abertura do papel-mercado no pregão.
             Min DECIMAL(13,2), preço mínimo do papel-mercado no pregão.
             Max DECIMAL(13,2), preço máximo do papel-mercado no pregão.
             Fech DECIMAL(13,2), preço do último negócio do papel-mercado no pregão.
             Neg MEDIUMINT(5), número de negócios efetuados com o papel-mercado no pregão.
             VolQuan BIGINT, quantidade total de títulos negociados neste papel-mercado.
             VolDinh DECIMAL(18,2), volume total de dinheiro negociado neste papel-mercado.

        """
        try:
            self.cursor.execute("CREATE TABLE Cotacoes (\
                                 id SMALLINT UNSIGNED NOT NULL,\
                                 Data DATE NOT NULL,\
                                 Cod CHAR(12),\
                                 Aber CHAR(13),\
                                 Min CHAR(13),\
                                 Max CHAR(13),\
                                 Fech CHAR(13),\
                                 Neg CHAR(5),\
                                 VolQuan CHAR(18),\
                                 VolDinh DECIMAL(18,2),\
                                 FOREIGN KEY(id) REFERENCES CS(id)\
                                 )"
                                )
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)

    def fillCotacoes(self, dirdb = '.', fileList = ['b*.xls', 'p*.xls']):
        """Fill Cotacoes table with data from metastock file took from grafix software.

        """

        try:
            fileList = list(Globcp().iglobcp(fileList))#modified glob function to catch list of files like ['*.doc', '*.xls']

            for xfile in fileList:
                stock = open(xfile, 'r') #open metastock file

                query = "SELECT id FROM CS WHERE Codigo = \'%s\'" % (xfile[:-1])
                self.cursor.execute(query)
                CSid = self.cursor.fetchall()[0][0]

                cota = {'id':'',
                        'Data':'',
                        'Cod':'',
                        'Aber':'',
                        'Min':'',
                        'Max':'',
                        'Fech':'',
                        'Neg':'',
                        'VolQuan':'',
                        'VolDinh':''}
                cota['id'] = CSid
                cota['Cod'] = xfile

                for pregao in stock:
                    try:
                        cota['Data'],\
                        cota['Aber'],\
                        cota['Min'],\
                        cota['Max'],\
                        cota['Fech'],\
                        cota['Neg'],\
                        cota['VolQuan'],\
                        cota['VolDinh'] = pregao.split()

                        cota['Data'] = '-'.join(parseDate(cota['Data'])) #dd/mm/yyyy --> yyyy-mm-dd

                        query = "INSERT INTO Cotacoes (%s)\
                                    VALUES %s" % (','.join(cota.keys()), str(tuple(cota.values())))
                        self.cursor.execute(query)
                    
                    except MySQLdb.Error, e:
                        print "Error %d: %s" % (e.args[0], e.args[1])
                        sys.exit (1)
        except:
            print "Se deu mal!!!"

##############Falta fazer o update!!!!!!!!!!****************
    def updateCotacoes(self, dirdb = '.', fileList = ['*']):
        try:
            fileList = list(Globcp().iglobcp(fileList))#modified glob function to catch list of files like ['*.doc', '*.xls']


            for xfile in fileList:
                stock = open(xfile, 'r') #open metastock file

                query = "SELECT id FROM CS WHERE Codigo = \'%s\'" % (xfile[:-1])
                self.cursor.execute(query)
                CSid = self.cursor.fetchall()[0][0]

                cota = {'id':'',
                        'Data':'',
                        'Cod':'',
                        'Aber':'',
                        'Min':'',
                        'Max':'',
                        'Fech':'',
                        'Neg':'',
                        'VolQuan':'',
                        'VolDinh':''}
                cota['id'] = CSid
                cota['Cod'] = xfile

                for pregao in stock:
                    try:
                        cota['Data'],\
                        cota['Aber'],\
                        cota['Min'],\
                        cota['Max'],\
                        cota['Fech'],\
                        cota['Neg'],\
                        cota['VolQuan'],\
                        cota['VolDinh'] = pregao.split()

                        cota['Data'] = '-'.join(parseDate(cota['Data'])) #dd/mm/yyyy --> yyyy-mm-dd

                        query = "INSERT INTO Cotacoes (%s)\
                                    VALUES %s" % (','.join(cota.keys()), str(tuple(cota.values())))
                        self.cursor.execute(query)
                    
                    except MySQLdb.Error, e:
                        print "Error %d: %s" % (e.args[0], e.args[1])
                        sys.exit (1)
        except:
            print "Se deu mal!!!"
        




##class Dividendos:
##    #dividendos e juros sobre capital proprio
##
    ###INDICADORES DE INVESTIMENTO
    #P = preco da acao
    #NA = numero total de acoes no mercado
    #PL = patrimonio liquido
    #LL = lucro liquido
    #VM = valor de mercado da empresa = P x NA
    #RPL = retorno sobre patrimonio liquido = LL / PL
    #LPA = lucro por acao = LL / NA
    #PLA = patrimonio liquido por acao = PL / NA
    #P/L (PE) = preco/lucro = P / LPA
    #P/PLA (PPLA) = preco/patrimonio liquido 
    #DA = dividendo por acao = D / NA
    #PO = payout = D / LL
    #YD = yield do dividendo = DA / PA
    #P/FC = preco/fluxo de caixa

 
    def P(self, company, date):
        query = "SELECT PreUlt FROM Cotacoes WHERE (Data = '%s') AND (CodNeg = '%s')" % (date, company)
        self.cursor.execute(query)
        P = self.cursor.fetchall()[0][0]
        return float(P[:-2] + '.' + P[-2:])

    def NA(self, company, date):
        pass

##    def PL(self, company, date):
##        query = "SELECT `2.04` FROM %s\
##                WHERE id=%s AND Trimestre=%s" % ('BP', , )
##        self.cursor.execute(query)
##        PL = self.cursor.fetchall()[0][0]
##        return float(PL[:-2] + '.' + PL[-2:])

    def LL(self, code, date):
        query = "SELECT id, itr FROM CS\
                WHERE Codigo = \'%s\'" % (code,)
        self.cursor.execute(query)
        a = self.cursor.fetchall()

        query = "SELECT %s FROM ITR%d\
                WHERE id = %d AND Trimestre = \'%s\'" % (('`3.15`', '`3.13`')[(a[0][1]-1)], a[0][1], a[0][0], date)
        self.cursor.execute(query)
        a = self.cursor.fetchall()
        return a[0][0]



    ###INDICADORES FINANCEIROS
    #AC = ativo circulante
    #PC = passivo circulante
    #DC = divida de curto prazo
    #DL = divida de longo prazo (incluindo debentures)
    #CB = disponivel (caixa, bancos e aplicacoes financeiras liquidas)
    #PM = participacao minoritaria
    #PL = patrimonio liquido
    #IR = imposto de renda
    #DF = despesas financeiras
    #LL = lucro liquido
    #TA = total do ativo
    #LB = lucro bruto
    #RL = receita liquida
    #LO = lucro operacional
    #RF = receitas financeiras
    #JC = juros sobre capital proprio
    #D/PL = endividamento liquido/patrimonio liquido = (DC + DL - DB) / (PL + PM)
    #LC = liquidez corrente = AC/PC
    #CDF = cobertura de despesas financeiras = (LL + IR + DF) / DF
    #RA = retorno sobre ativo = (LL + PM) / TA
    #MB = margem bruta = LB / RL
    #MA = margem na atividade = (LO + DF - RF + JC) / RL
    #ML = margem liquida = LL / RL



##class FCD:



##class GenericDBOP:
##    def __init__(self, db, name):
##        self.db = db #database connection
##        self.name = name #table name
##        self.dbc = self.db.cursor() #cursor object
##        self.debug=1
##
##    def __getitem__(self, item):
##        self.dbc.execute("select * from %s limit %s, 1" % (self.name, item))
##        return self.dbc.fetchone()
##
##    def _query(self, q):
##        if self.debug: print "Query: %s" % (q)
##        self.dbc.execute(q)
##
##    def __iter__(self):
##        "creates a data set, and returns an iterator (self)"
##        q = "select * from %s" % (self.name)
##        self._query(q)
##        return self  # an Iterator is an object 
##                                        # with a next() method
##
##    def next(self):
##        "returns the next item in the data set, or tells Python to stop"
##        r = self.dbc.fetchone()
##        if not r:# Ok here error is handled and rethrown
##            raise StopIteration
##        return r


    
    
def main():
    p= 'C:\\Users\\oikawa\\Documents\\Ações\\projeto SQL\\teste'
    m='C:\\Users\\oikawa\\Documents\\Ações\\projeto SQL\\balanços\\capitalsocial.xls'
#    unzip_files(p)
#    createCS(testuser='root', testpass='blablabla')
#    fillCS(modelo=m,testuser='root', testpass='blablabla')
#    createBP_DR(testuser='root', testpass='blablabla')
#    xls2mysql(dirdb=p, testuser='root', testpass='blablabla')


    
if __name__ == '__main__': main()

