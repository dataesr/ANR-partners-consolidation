#importer les packages
import requests
import pandas as pd
import numpy as np
from matcherANR import identifie_structure,identifiant_prefere,replace_all,envoi_scanR,subset_df,res_futures, replace_accent, reunir_personnes
from tqdm.notebook import tqdm
tqdm.pandas()

########################################### amener les json depuis le site de l'anr #########################################################

url_projets_0509="https://www.data.gouv.fr/fr/datasets/r/a16e0fd7-a008-499b-bbd3-b640f8651bd9"
url_projets_10="https://www.data.gouv.fr/fr/datasets/r/afe3d11b-9ea2-48b0-9789-2816d5785466"
url_partenaires_0509="https://www.data.gouv.fr/fr/datasets/r/18e345ee-7a16-4727-8ac5-b237db974e24"
url_partenaires_10="https://www.data.gouv.fr/fr/datasets/r/9b08ee21-7372-47a4-9831-4c56a8099ee8"

page_projets_0509 = requests.get(url_projets_0509).json()
page_projets_10 = requests.get(url_projets_10).json()
page_partenaires_0509 = requests.get(url_partenaires_0509).json()
page_partenaires_10 = requests.get(url_partenaires_10).json()

#on prend les noms des colonnes
colonnes_projets_0509 = page_projets_0509['columns']
colonnes_projets_10 = page_projets_10['columns']
colonnes_partenaires_0509 = page_partenaires_0509['columns']
colonnes_partenaires_10 = page_partenaires_10['columns']

#et leur donnees associees
donnees_projets_0509 = page_projets_0509['data']
donnees_projets_10 = page_projets_10['data']
donnees_partenaires_0509 = page_partenaires_0509['data']
donnees_partenaires_10 = page_partenaires_10['data']

#on les met sous forme de dataframes
df_projets_0509=pd.DataFrame(data=donnees_projets_0509,columns=colonnes_projets_0509)
df_projets_10=pd.DataFrame(data=donnees_projets_10,columns=colonnes_projets_10)
df_partenaires_0509=pd.DataFrame(data=donnees_partenaires_0509,columns=colonnes_partenaires_0509)
df_partenaires_10=pd.DataFrame(data=donnees_partenaires_10,columns=colonnes_partenaires_10)

df_partenaires=pd.concat([df_partenaires_0509,df_partenaires_10])
df_partenaires['index']=[x for x in range(len(df_partenaires))]
df_partenaires=df_partenaires.set_index('index')
df_partenaires=df_partenaires[['Projet.Code_Decision_ANR','Projet.Partenaire.Code_Decision_ANR','Projet.Partenaire.Est_coordinateur', 'Projet.Partenaire.Nom_organisme','Projet.Partenaire.Responsable_scientifique.Nom','Projet.Partenaire.Responsable_scientifique.Prenom']]

df_projets=pd.concat([df_projets_0509,df_projets_10])
df_projets['index']=[x for x in range(len(df_projets))]
df_projets=df_projets.set_index('index')

########################################### traitement donnees des partenaires aux projet de recherche #########################################################

############ matcher structures avec differentes sources de donnees

######## matcher Anne et Eric
url="https://affiliation-matcher.staging.dataesr.ovh/match" #url matcher structure

df_partenaires['Projet.Partenaire.Nom_organisme']=df_partenaires.loc[:,'Projet.Partenaire.Nom_organisme'].apply(lambda x: str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h"))
df_partenaires['id_structure_matcher']=df_partenaires.progress_apply(lambda row: identifie_structure(row), axis=1)

#on stocke
df_partenaires.to_excel('df_partenaires.xlsx')
df_partenaires.to_json('df_partenaires.json')


df_partenaires=pd.read_excel('df_partenaires.xlsx')
df_partenaires['id_structure_matcher']=df_partenaires.loc[:,'id_structure_matcher'].apply(lambda x: str(x).replace("['","").replace("']",""))


df_partenaires['Projet.Partenaire.Nom_organisme2']=df_partenaires.loc[:,'Projet.Partenaire.Nom_organisme'].apply(lambda x: replace_all(str(x).lower()))
df_partenaires_struct=df_partenaires#[['Projet.Partenaire.Nom_organisme','Projet.Partenaire.Nom_organisme2','Projet.Partenaire.Nom_organisme3','Projet.Partenaire.Code_RNSR','id_structure_matcher']]
####### récupération des identifiants sur scanR
url_scanr='https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/scanR/projects.json'

scanR_nettoye=pd.read_excel('df_partenaires_scanR.xlsx')
scanR_nettoye['Projet.Partenaire.Nom_organisme2']=scanR_nettoye['Projet.Partenaire.Nom_organisme'].apply(lambda x: replace_all(str(x).lower()))
scanR_nettoye=scanR_nettoye.drop_duplicates(subset='Projet.Partenaire.Nom_organisme2')
scanR_nettoye=scanR_nettoye[['Projet.Partenaire.Nom_organisme2','id_structure_scanr']]
df_partenaires_struct=pd.merge(df_partenaires_struct,scanR_nettoye, on='Projet.Partenaire.Nom_organisme2', how='left')

####### récupération des identifiants trouvés par Emmanuel
scanr_part_nn_id=pd.read_excel('scanr_partenaires_non_identifies.xlsx')
scanr_part_nn_id['Projet.Partenaire.Nom_organisme2']=scanr_part_nn_id.loc[:,'Nom'].apply(lambda x: replace_all(str(x).lower()))
scanr_part_nn_id=scanr_part_nn_id.drop_duplicates(subset='Projet.Partenaire.Nom_organisme2')
repechage=pd.merge(df_partenaires_struct,scanr_part_nn_id, on='Projet.Partenaire.Nom_organisme2', how='left')
repechage['index']=[x for x in range(len(repechage))]
repechage=repechage.set_index('index')

####### récupération des identifiants trouvés par Emmanuel la 2eme fois
pas_trouve_maj=pd.read_excel('pas_trouve_maj.xlsx')
pas_trouve_maj['Projet.Partenaire.Nom_organisme2']=pas_trouve_maj.loc[:,'Projet.Partenaire.Nom_organisme'].apply(lambda x: replace_all(str(x).lower()))
pas_trouve_maj=pas_trouve_maj[['Projet.Partenaire.Nom_organisme2','id']]
pas_trouve_maj=pas_trouve_maj.drop_duplicates(subset='Projet.Partenaire.Nom_organisme2')
repechage=pd.merge(repechage,pas_trouve_maj, on='Projet.Partenaire.Nom_organisme2', how='left')


#######rassemblement
repechage['id_structure']=repechage.apply(lambda row: identifiant_prefere(row), axis=1)
repechage.to_excel('df_partenaires_structures.xlsx')

###matcher checheurs

df_partenaires = pd.read_excel('df_partenaires_structures.xlsx')
df_partenaires['index2']=[str(x) for x in range(len(df_partenaires))]
dict_subset_df = subset_df(df_partenaires)
df_id_personne = res_futures(dict_subset_df)

df_partenaires_identifies=pd.merge(df_partenaires, df_id_personne,left_on='Projet.Partenaire.Responsable_scientifique.Nom', right_on='nom', how='left')
df_partenaires_identifies=df_partenaires_identifies.drop_duplicates(subset='index2')
df_partenaires_identifies=df_partenaires_identifies[[ 'Projet.Code_Decision_ANR', 'Projet.Acronyme',
       'Projet.Partenaire.Code_Decision_ANR',
       'Projet.Partenaire.Est_coordinateur', 'Projet.Partenaire.Nom_organisme',
       'Projet.Partenaire.Categorie_organisme',
       'Projet.Partenaire.Responsable_scientifique.Nom',
       'Projet.Partenaire.Responsable_scientifique.Prenom',
       'Projet.Partenaire.Adresse.Ville', 'Projet.Partenaire.Adresse.Region',
       'Projet.Partenaire.Adresse.Pays','index2','idref']]
df_partenaires_identifies.to_excel('df_partenaires_identifies.xlsx')

#on envoie le tout sur scanR participations
df_partenaires=repechage
df_partenaires['project_type']=df_partenaires.apply(lambda x:'ANR', axis=1)
df_partenaires['role']=df_partenaires['Projet.Partenaire.Est_coordinateur'].apply(lambda x: 'coordinateur' if x == True else 'participant')
df_partenaires['index']=[x for x in range(len(df_partenaires))]
df_partenaires['id']=df_partenaires.apply(lambda row: str(row['Projet.Code_Decision_ANR'])+'-10'+str(row['index']), axis=1)
del df_partenaires['index']

df_partenaires=df_partenaires[['Projet.Code_Decision_ANR','Projet.Partenaire.Nom_organisme','id', 'id_structure', 'project_type', 'role']]

df_partenaires=df_partenaires.rename(columns={'Projet.Code_Decision_ANR':'project_id'})
df_partenaires=df_partenaires.rename(columns={'Projet.Partenaire.Nom_organisme':'name_source'})
df_partenaires=df_partenaires.rename(columns={'id_structure':'participant_id'})

df_partenaires.to_excel('df_participants.xlsx')
df_partenaires=pd.read_excel('df_participants.xlsx')
del df_partenaires['Unnamed: 0']
df_partenaires['participant_id']=df_partenaires.loc[:,'participant_id'].apply(lambda x: x if x is not np.nan else 'None')

df_partenaires.loc[2,:].to_json()

df_partenaires.iloc.apply(lambda row: envoi_scanR('http://185.161.45.213/projects/participations',row),axis=1)


#on envoie sur scanR projets

df_partenaires_identifies=pd.read_excel('df_partenaires_identifies.xlsx')
df_partenaires_identifies['role']=df_partenaires_identifies['Projet.Partenaire.Est_coordinateur'].apply(lambda x: 'coordinateur' if x == True else 'participant')
df_partenaires_identifies['identified']=df_partenaires_identifies['idref'].apply(lambda x: str(x)[0:5]=='idref')

df_partenaires_identifies=(df_partenaires_identifies.groupby(['Projet.Code_Decision_ANR'])
                            .agg({'identified': lambda x: x.tolist(),
                                  'Projet.Partenaire.Responsable_scientifique.Nom': lambda x: x.tolist(),
                                  'Projet.Partenaire.Responsable_scientifique.Prenom': lambda x: x.tolist(),
                                  'role': lambda x: x.tolist(),
                                  'idref': lambda x: x.tolist()})
                            .rename({'Projet.Partenaire.Responsable_scientifique.Nom' : 'last_name',
                                    'Projet.Partenaire.Responsable_scientifique.Prenom' : 'first_name',
                                    'idref' : 'id'},axis=1).reset_index())

df_partenaires_identifies['last_name']=df_partenaires_identifies.loc[:,'last_name'].apply(lambda x: replace_accent(x))
df_partenaires_identifies['first_name']=df_partenaires_identifies.loc[:,'first_name'].apply(lambda x: replace_accent(x))

df_partenaires_identifies['persons']=df_partenaires_identifies.apply(lambda row: reunir_personnes(row) if row['first_name'] is not np.nan else None, axis=1)
df_projets_partenaires=pd.merge(df_projets,df_partenaires_identifies, on='Projet.Code_Decision_ANR', how='left' )




df_partenaires.apply(lambda row: envoi_scanR('http://185.161.45.213/projects/projects',row),axis=1)














