import requests
import pandas as pd
import numpy as np
from retry import retry
import concurrent.futures
import logging
import threading
import sys
import ast
from pydref import Pydref
#url matcher structure
url="https://affiliation-matcher.staging.dataesr.ovh/match"


#fonction identifie structure
def identifie_structure(row):
    f= f"{row['Projet.Partenaire.Nom_organisme']} {row['Projet.Partenaire.Adresse.Ville']} {row['Projet.Partenaire.Adresse.Pays']} "
    rnsr=requests.post(url, json= { "query" : f , "type":"rnsr", "year": int("20"+row['Projet.Code_Decision_ANR'][4:6])})
    ror=requests.post(url, json= { "query" : f , "type":"ror"})
    grid=requests.post(url, json= { "query" : f , "type":"grid"})
    result_rnsr=rnsr.json()['results']
    result_ror=ror.json()['results']
    result_grid=grid.json()['results']                               
    if result_rnsr != []:
        print (result_rnsr)
        return result_rnsr
    elif result_rnsr == [] and result_grid != []:
        print (result_grid)
        return result_grid
    elif result_rnsr == [] and result_grid == [] and result_ror != []:
        print (result_ror)
        return result_ror
    else:
        print (None)
        return None

#fonctions qui identifie les personnes


def get_logger(name):
    loggers = {}
    if name in loggers:
        return loggers[name]
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fmt = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    loggers[name] = logger
    return loggers[name]


def res_futures(dict_nb):
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=11, thread_name_prefix="thread") as executor:
        # Start the load operations and mark each future with its URL
        future_to_req = {executor.submit(query, df): df for df in dict_nb.values()}
        for future in concurrent.futures.as_completed(future_to_req):
            req = future_to_req[future]
            try:
                data = future.result()
                res.append(data)
                jointure = pd.concat(res)
            except Exception as exc:
                print('%r generated an exception: %s' % (req, exc), flush=True)

    return jointure


@retry(tries=3, delay=5, backoff=5)
def query(df):
    pydref = Pydref()
    logger = get_logger(threading.current_thread().name)
    logger.info("start")
    part_idref = {"nom": [], "prenom": [],"code_anr":[], "idref": []}
    for _, r in df.iterrows():
        try:
            name = r['Projet.Partenaire.Responsable_scientifique.Prenom'] + " " + r['Projet.Partenaire.Responsable_scientifique.Nom']
            result = pydref.identify(name)
            if result.get("status") == "found" and result['idref']!='idref073954012':
                part_idref["nom"].append(r['Projet.Partenaire.Responsable_scientifique.Nom'])
                part_idref["prenom"].append(r['Projet.Partenaire.Responsable_scientifique.Prenom'])
                part_idref["code_anr"].append(r['Projet.Code_Decision_ANR'])
                part_idref["idref"].append(result.get("idref"))
        except:
            pass

    df = pd.DataFrame(data=part_idref)
    logger.info("end")

    return df


def subset_df(df):
    prct10 = int(round(len(df) * 10 / 100, 0))
    dict_nb = {}
    deb = 0
    fin = prct10
    dict_nb["df1"] = df.iloc[deb:fin, :]
    deb = fin + 1
    dixieme = 10 * prct10
    reste = (len(df) - dixieme)
    fin_reste = len(df) + 1
    for i in range(2, 11):
        fin = (i * prct10 + 1)
        dict_nb["df" + str(i)] = df.iloc[deb:fin, :]
        if reste > 0:
            dict_nb["reste"] = df.iloc[fin: fin_reste, :]
        deb = fin

    return dict_nb


#fonction qui hiérarchise les identifiants selon la préférance
def identifiant_prefere(row):
    if str(row['id']) != 'None' and str(row['id']) != 'NaN' and row['id'] is not np.nan :
        return row['id']
    elif str(row['Projet.Partenaire.Code_RNSR']) != 'None' and str(row['Projet.Partenaire.Code_RNSR']) != 'NaN' and row['Projet.Partenaire.Code_RNSR'] is not np.nan :
        print('ok')
        return row['Projet.Partenaire.Code_RNSR']
    elif str(row['id_structure_matcher']) != 'None' and str(row['id_structure_matcher']) != 'NaN' and row['id_structure_matcher'] is not np.nan :
        return row['id_structure_matcher']
    elif str(row['id_structure_scanr']) != 'None' and str(row['id_structure_scanr']) != 'NaN' and row['id_structure_scanr'] is not np.nan :
        return row['id_structure_scanr']
    elif str(row['code']) != 'None' and str(row['code']) != 'NaN' and row['code'] is not np.nan :
        return row['code']
    else:
        return None
    
#fonction pour éliminer les doublons inintéressants pour Emmanuel

def doublons(df,row):
    s=0
    for i in list(df.loc[row.index[0]:,'Projet.Partenaire.Nom_organisme2']):
        if i == row['Projet.Partenaire.Nom_organisme2']:
            s+=1
    if s > 1:
        if row['id_structure'] is np.nan or str(row['id_structure']) == 'None' or str(row['id_structure']) == 'NaN':
            return True
        else:
            print('ok')
            return False
    else:
        print('ok')
        return False

def pas_trouve(x):
    if x is np.nan or str(x) == 'None' or str(x) == 'NaN':
        return True
    else:
        print('ok')
        return False
    
#fonction pour donner un identifiant a ceux qui en ont pas

def attribue_id(row,df):
    for i in range (len(df)):
        if row['Projet.Partenaire.Nom_organisme2']==list(df.loc[:,'Projet.Partenaire.Nom_organisme2'])[i] and (df.loc[i,'id_structure'] is np.nan or str(df.loc[i,'id_structure']) == 'None' or str(df.loc[i,'id_structure'] == 'NaN')):
            row['id_structure']=df.loc[i,'Projet.Partenaire.Nom_organisme2']
        else:
            row['id_structure']= None
  
#fonction qui remplace 

dic={" - japon":"","(vub)":"","d'hebron":""," (south africa)":"",
     "university of wageningen / biochemistry":"univeritywageningen"," - suède":"",
     " upv/ehu":"","rome la sapienza":"romaapienza","pensylvania":"penylvania",
     "isamail":"ismail","(unimib)":"","goteborg":"gothenburg","eastern finlande":"eaternfinland",
     "copenhaguen":"copenhague","colombia":"columbia","bayereuth":"bayreuth",
     "stendhal grenoble iii":"tendhalgrenoble3","université grenoble 1":"univeritegrenoblei",
     "essone":"eonne"," d’":"","montpelleir":"montpellier","lisbone":"libonne","ferrand 1":"ferrand",
     "diop de dakar":"diop","polite`cnica":"politecnica","polite`cnica":"politecnica",
     " milano":"","(ucsc)":"","(upv/ehu)":"","(ungda)":"","mannar":"manar","¨":""," sarl":"","(sgn)":"",
     "(sruc)":"","sapienza università di roma":"apienzauniverit?diroma"," cree:":"",": london":"",
     "(nioz)":"","de l' est (ppe)":"etppe","(necs)":"","veterinay":"veterinary","inst.":"intitute"," torun":"",
     "research and development":"rd"," -imnr":"","rresearch":"reearch","(rivm)":""," netherlands":"",
     " heath ":"health","for cell biology & genetics":""," für ":"","chaft zur":"",
     "universität universität münchen":"univeritymunich","(ldo)":"","(licsen)":"","envirionnement":"environnement",
     " ag: allemagne":"ag","kbs":"kedgebuinechool","für technologie - allemagne":"furtechnologie",
     "jozef stefan institut":"jozeftefanintitute",": instituto superior técnico":"","(ibet): portugal":"","(ist austria)":"",
     "(iciq) - espagne":"tarragona","institute national de ":"intitutnational"," (irb barcelona)":"",
     "institute for quantum optics and quantum information of the austrian academy of sciences":"intitutequantumopticquantuminformationautrianacamyciencevienna",
     "bioenginnering":"bioenginering","(ilvo)":"","(ipc)":""," nationall ":"nationale"," (irstea)":"",
     "institut national de recherche en sciences et technologies pour lenvironnement et de lagriculture":"intitutnationalrechercheciencetechnologiquelenvironnmentlagriculture",
     "institut national de recherche en sciences et technologies de lenvironnement et de lagriculture":"intitutnationalrechercheciencetechnologiquelenvironnmentlagriculture",
     "institut national de recherche en sciences et technoligies pour l'environnement et l'agriculture":"intitutnationalrechercheciencetechnologiquelenvironnmentlagriculture",
     "l'infromation":"linformation"," du ":"","institut français du textile et de l'habillement de paris":"intitutfrancaidutextileethabillement",
     "usr 3337 amérique latine":"ur3337","institut economie scientifique gest":"intituteconomiecientifiquegetionieeg",
     "doptique":"optique","de médenine":"tuniie","/arid regions institut":"tuniie","direction régionale":"dr","biologie structural":"biologietructurale",
     "délégation régionale":"dr","delegation regionale":"dr","inserm - délégation régionale provence alpes côte dazur et corse":"inermdrprovencealpecoteazurcorse",
     " dazur":"azur","(imim)":"","hospital universitario vall d'hebrón":"hopitaluniveritarivallhebron","faculty of medicine":"medicalfaculty",
     "german center for neurodegenerative diseases- munich":"germancenterneurodegenerativedieae","(dzne)":"",": inserm umr s_910":"umrs910",
     "génétique médical":"genetiquemedicale","_":"",": icn2 (csic & bist)":"","à":"a","(icn2)":"","mach: research and innovation centre":"mach",
     "â":"a","zuerich":"zurich","rotterdam - emc":"","ecole supérieure d'informatique: electronique etautomatique":"ecoleuperieuredinformatiqueelectroniqueautomatique",
     "féréérale":"federale"," lausane":"lausanne","ecole polytechnique federal":"ecolepolytechniquefederale","d’alger":"alger","d'ingenieurs":"dingenieur",
     "d'armement":"darmement","alger":"algerie","algiers alger":"algerie","(siem reap)":"",
     "department of computing: imperial college london":"departmentcomputingimperialcollege",
     "council for agriculture research and economics":"councilagriculturalreearcheconomics",
     "(idibaps)":"","(imm)":"","(cnr)":"","(csic)":"","z":"","pyrenees":"pyrenee","(cut)":"","de aragón":"",
     "de aragon":"","tecnloco":"tecnologico","del instituto politecnico nacional":"","/ université de brasilia":"","- cnes":"",
     "_bioénergétique et ingénierie des protéines":"bip","de sanaa":"","(crg)":"","dexperimentation":"experimentation",
     " (cete med )":""," - umifre n°16":"","detudes":"etude","physcis":"physics",
     "bilkent university - department of computer engineering":"bilkentuniverity","(beia)":""," - turquie":"","(ait)":"",
     "atominstitut techniche universität wien":"atomintituttechnicaluniverityvienna","areva stockage denergie":"arevatokagedenergie",
     "(apha)":"","alfred-wegener institute: helmholtz center for polar and marine science":"alfredwegenerintitute",
     "alfred wegener institute: helmholtz-zentrum für polar- und meeresforschung (awi)":"alfredwegenerintitute",
     "a2ia":"a2iaanalyeimageintelligenceartificielle","\xa0":"","ifremer - centre de nantes":"ifremer nantes",
     "humboldt-university:":"humboldtuniveritatzuberlin","humboldt university of berlin":"humboldtuniveritatzuberlin",
     "humboldt university berlin / institute of biology: experimental biophysics":"humboldtuniveritatzuberlin",
     "humboldt university berlin":"humboldtuniveritatzuberlin",
     "humboldt institute for internet and society":"humboldtuniveritatzuberlin","(hgugm)":"","(roumanie)":"",
     "hôpital européen g. pompidou: service of microbiology":"hopitaleuropeengeorgepompidou","hokkeido":"hokkaido",
     "helmholz zentrum münchen":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum münchen: german research center for environmental health / research unit analytical biogeochemistry":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum münchen german research center for environmental health":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum münchen":"helmholtzzentrummunchenmunich","helmholtz zentrum muenchen gmbh":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum muenchen - german research center for environmental health (gmbh)":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum muenchen":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum muenchen - german research center for environmental health (gmbh)":"helmholtzzentrummunchenmunich",
     "helmholtz zentrum münchen – deutsches forschungszentrum für gesundheit und umwelt gmbh (hmgu).":"helmholtzzentrummunchenmunich",
     "muenchen":"munchen"," gmbh ":"","ufz - allemagne":""," ufz ":""," gfz ":"","organization - demeter":"organiationdemeter",
     "(hao)":"","duesseldorf":"","heinrich-heine university dusseldorf":"heinrichheineuniveritat",
     "universität düsseldorf":"univeritatdueldorf","hebrew university hospital":"hebrewuniverity","medical faculty":"facultymedecine",
     "goethe-universität":"goetheuniveritatfrankfurt","enst bretagne":"ent","/ gesis":"","(idiv)":"","(dzne)":"","(dkfz)":"",
     "georg-august university - allemagne":"georgaugutuniveritygottingen","inserm umr s_910":"umrs910",
     "génétique médical":"genetiquemedicale"," inc.":"","(gmit)":"","invesztigacion":"investigacion",": icn2 (csic & bist)":"",
     "(icn2)":"","fundacio hospital universitari vall d’hebron (huvh) – institut de recerca (vhir)/ fundacio privada institut d’investigacio oncologica de vall d’hebron (vhio)":"fundaciohopitaluniveritarivallhebronintitutrecercafundacioprivadaintitutdinvetigaciooncologicavalldhebronvhio",
     "(hcb)":"","(fcrb)":"","biom?dica (fcrb) ? hospital clinic de barcelona":"","ce3c":"","t jena":"t","(fli)":"",
     "friedrich-alexander-universität":"friedrichalexanderuniverityerlangennuremberg","nümberg":"nuremberg",
     "french national scientific research center (cnrs)":"cnrs","universitaet":"universitat","(fsl)":"","'n'":"|n|",
     " i n i ":"|n|","fraunhofer ise":"fraunhoferintituteolarenergyystem","s ise":"","fraunhofer institute (fhg) -":""," e.v.":"",
     "foerderung":"forderung","(fist sa)":"","scientique":"scientifique","(fuel)":"",
     "foundation neurological institute c. besta":"fondazioneintitutoneurologicocarlobeta","(forth)":"",
     "foundation carlo besta neurological institute":"fondazioneintitutoneurologicocarlobeta","forshungszentrum":"forchungszentrum",
     "juelich":"julich","/cncs":"","irccs":"",": milan":""," carlo ":"c","istituto":"instituto","di milano - int":"","(indt)":"",
     "(int)":"","(sciences po)":""," sceinces":"sciences"," tse ":"","laffont toulouse sciences economiques":"laffont",
     "ujf-filiale":"filialeujf","flemish":"flemisch"," environmental institute":"institute of environment",
     " environment institute":"institute of environment","(fr)":"","marterials":"material"," the ":"",
     "univ porto (up)":"univerityporto","(cu)":""," resear ":"reearch","insitute":"institute"," hysical ":"physical",
     "vuinérables":"vulnerable","ées":"ee","- electricite de france":"","electricite de france -":"","electricité de france":"edf",
     "electricite de france":"edf","ville de paris":"pari","espci":"","gif-sur-yvette":"","electricite (sup":"electriciteupelec",
     "federal":"federale","(ens)":"","(oniris)":"","(ensv)":""," st ":"saint"," sant-":"saint","(ensm":"(ensma)","(ensa)":"",
     "(ean)":"","stras":"trabourg","clermont f":"clermontferrand","chauss":"chauee","(enpc)":"","(enac)":"",
     "traiement":"traitement","(ad2m)":"","(anses)":"","":"",
     "microtenhique":"microtechnique","microorgnismes":"microorganismes","università di cagliari":"universityofcagliari",
     "artctique":"arctique","besa":"beancon"," dele ":"delle","nazionalle":"nazionale",
     "alternaltive":"alternative","pyre":"pyrenee",".":"","scienctifique":"cientifique","agronomiqu":"agronomique",
     "besanco":"beancon","ème siècle":"eiecle","observatoie":"obervatoire","macromoléccules":"macromolecule","lyo":"lyon",
     "public et privé":"privepublic","structuale":"tructurale","wageningingen":"wageningen","minères":"miniere","(":"",")":"",
     "archéozzologie":"archeozoologie","alimentantion":"alimentation","sudorium":"tudorium",
     "ë":"e","ü":"u","i'":"l",":":""," te ":"","ò":"o"," i ":""," for ":"","ä":"a"," de ":"",
     "part":""," of ":""," en ":""," pour ":"","s":"","&":""," & ":""," et ":"",
     " and ":""," un ":""," une ":"",":":"","ó":"o"," à ":"a","í":"i",",":"",
     "ç":"c","û":"u","ê":"e","é":"e","è":"e",
     "à":"a","â":"a","ô":"o","î":"i"," de ":""," da":""," de":""," di":""," do":""," du":""," dh":"",
     " d'a":""," d'e":""," d'i":""," d'o":""," d'u":""," d'h":"",
     " d´a":""," d´e":""," d´i":""," d´o":""," d´u":""," d´h":"",
     " l'a":""," l'e":""," l'i":""," l'o":""," l'u":""," l'h":"",
     " l´a":""," l´e":""," l´i":""," l´o":""," l´u":""," l´h":"",
     " la":""," le":""," li":""," lo":""," lu":""," lh":"",
     "’":"","´":"","–":"","/":"",":":"","-":"","'":""," ":"","et":"","de":""}

def replace_all(row):
    for i, j in dic.items():
        row = row.replace(i, j)
    return row

def replace_accent(x):
    for i in range(len(x)):
        x[i]=str(x[i]).replace("'"," ").replace("´"," ").replace("’"," ")
    return x

# fonction qui envoie les données sur scanR

def envoi_scanR(url,row):
    try:
        print(row.to_dict())
        r=requests.post(url, json = row.to_dict() ,headers={"Content-Type":"application/json",'Authorization': 'Basic cm9vdDp0b25uZXJyZTJCcmVzdA=='})
        print('ok')
        print(r.status_code)
        print(r.json())
    except Exception  as e:
        print(e)
    

#fonction qui réunis les personnes d'un meme projet ANR

def reunir_personnes(row):
    dic={}
    if row['first_name'][0] is not np.nan or row['first_name'][0]!='nan':
        for i in range(len(row['first_name'])):
            d=str('{'+f"'identified':'{row['identified'][i]}','last_name':'{str(row['last_name'][i])}','first_name':'{str(row['first_name'][i])}','role':'{str(row['role'][i])}','id':'{str(row['id'][i])}'"+'}')
            d=ast.literal_eval(d)
            dic[i]= d
        return dic
    else:
        return None
    











    
